import com.sun.source.tree.*;
import com.sun.source.util.JavacTask;
import com.sun.source.util.SourcePositions;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

/** Read-only structural comparison. Exemptions come only from course/source-parity.tsv. */
public final class SourceParity {
    private static final String HEADER = "kind\tside\tpath\tsymbol\treason";
    private static final String USAGE =
            "usage: java --source 21 scripts/SourceParity.java verify <repo-root> | self-test";
    private static final List<String> SIDES = List.of("reference", "exercises");

    record Rule(String kind, String side, String path, String symbol, int line) {}

    record Span(int start, int end, String replacement, Rule rule) {}

    record Decl(String symbol, Tree tree, long line, boolean privateMember) {}

    record Parsed(
            String source,
            CompilationUnitTree unit,
            SourcePositions positions,
            Map<String, List<Decl>> declarations) {}

    record Problem(boolean error, String key, String message) {}

    static final class Result {
        final List<Problem> problems = new ArrayList<>();
        int files;
        int bodies;
        int privateMembers;

        void error(String key, String message) {
            problems.add(new Problem(true, key, message));
        }

        void drift(String key, String message) {
            problems.add(new Problem(false, key, message));
        }

        int exitCode() {
            return problems.stream().anyMatch(Problem::error) ? 2 : problems.isEmpty() ? 0 : 1;
        }

        String diagnostics() {
            return problems.stream()
                    .sorted(Comparator.comparing(Problem::key).thenComparing(Problem::message))
                    .map(
                            p ->
                                    "source-parity: "
                                            + (p.error ? "ERROR " : "DRIFT ")
                                            + p.key
                                            + ": "
                                            + p.message)
                    .collect(Collectors.joining("\n"));
        }

        void print() {
            if (exitCode() == 0) {
                System.out.printf(
                        "source-parity: OK (files=%d, method-bodies=%d, private-members=%d)%n",
                        files, bodies, privateMembers);
            } else {
                System.err.println(diagnostics());
            }
        }
    }

    public static void main(String[] args) {
        int code;
        try {
            if (args.length == 1 && args[0].equals("self-test")) {
                code = selfTest();
            } else if (args.length == 2 && args[0].equals("verify")) {
                Result result = verify(Path.of(args[1]));
                result.print();
                code = result.exitCode();
            } else {
                System.err.println("source-parity: ERROR invalid arguments\n" + USAGE);
                code = 2;
            }
        } catch (Exception e) {
            System.err.println(
                    "source-parity: ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage());
            code = 2;
        }
        System.exit(code);
    }

    static Result verify(Path root) {
        Result result = new Result();
        Map<String, Map<String, byte[]>> inputs = new TreeMap<>();
        for (String side : SIDES) {
            inputs.put(side, readFiles(root.resolve(side + "/src/main/java"), side, result));
        }
        String policy = "";
        Path path = root.resolve("course/source-parity.tsv");
        try {
            if (Files.isSymbolicLink(path))
                throw new IOException("policy must not be a symbolic link");
            policy = decode(Files.readAllBytes(path));
        } catch (IOException e) {
            result.error("course/source-parity.tsv", e.getMessage());
        }
        compare(inputs, policy, result);
        return result;
    }

    static Map<String, byte[]> readFiles(Path root, String side, Result result) {
        Map<String, byte[]> files = new TreeMap<>();
        try {
            // Reject links in the root path too, not just entries visited by Files.walk.
            for (Path p = root.toAbsolutePath(); p != null; p = p.getParent()) {
                if (Files.isSymbolicLink(p))
                    throw new IOException("symbolic link in source root: " + p);
            }
            if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("missing source directory: " + root);
            }
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted().toList()) {
                    String relative = root.relativize(path).toString().replace('\\', '/');
                    if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) continue;
                    if (Files.isSymbolicLink(path)
                            || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                        result.error(
                                side + "/" + relative,
                                "source entry is not a regular file (links are forbidden)");
                        continue;
                    }
                    try {
                        files.put(relative, Files.readAllBytes(path));
                    } catch (IOException e) {
                        result.error(side + "/" + relative, "cannot read: " + e.getMessage());
                    }
                }
            }
        } catch (IOException | java.io.UncheckedIOException e) {
            result.error(side, e.getMessage());
        }
        return files;
    }

    static String decode(byte[] bytes) throws CharacterCodingException {
        return StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }

    static boolean validPath(String path) {
        return path.endsWith(".java")
                && !path.startsWith("/")
                && !path.contains("\\")
                && !path.contains(":")
                && !path.contains("*")
                && !path.contains("?")
                && Arrays.stream(path.split("/", -1))
                        .noneMatch(p -> p.isEmpty() || p.equals(".") || p.equals(".."));
    }

    static List<Rule> readPolicy(String policy, Result result) {
        List<Rule> rules = new ArrayList<>();
        List<String> lines = policy.lines().toList();
        if (lines.isEmpty() || !lines.getFirst().equals(HEADER)) {
            result.error("course/source-parity.tsv", "expected header: " + HEADER);
            return rules;
        }
        Set<String> seen = new HashSet<>();
        for (int i = 1; i < lines.size(); i++) {
            String key = "course/source-parity.tsv:" + (i + 1);
            String[] columns = lines.get(i).split("\t", -1);
            if (columns.length != 5 || Arrays.stream(columns).anyMatch(String::isBlank)) {
                result.error(key, "expected five nonempty columns");
                continue;
            }
            String kind = columns[0], side = columns[1], path = columns[2], symbol = columns[3];
            if (!(kind.equals("method-body") && side.equals("both"))
                    && !(kind.equals("private-member") && SIDES.contains(side))) {
                result.error(key, "unknown kind/side: " + kind + "/" + side);
                continue;
            }
            if (!validPath(path)) {
                result.error(key, "invalid relative Java path: " + path);
                continue;
            }
            String identity = kind + "\t" + side + "\t" + path + "\t" + symbol;
            if (!seen.add(identity)) {
                result.error(key, "duplicate exemption: " + path + " " + symbol);
                continue;
            }
            rules.add(new Rule(kind, side, path, symbol, i + 1));
        }
        return rules;
    }

    static Parsed parse(String path, String source, Result result) {
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || Runtime.version().feature() < 21) {
            result.error(path, "Java 21+ JDK compiler required");
            return null;
        }
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        JavaFileObject file =
                new SimpleJavaFileObject(
                        URI.create("string:///Source.java"), JavaFileObject.Kind.SOURCE) {
                    @Override
                    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                        return source;
                    }
                };
        try (var manager =
                compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            var task =
                    (JavacTask)
                            compiler.getTask(
                                    null,
                                    manager,
                                    diagnostics,
                                    List.of("--release", "21", "-proc:none"),
                                    null,
                                    List.of(file));
            CompilationUnitTree unit = task.parse().iterator().next();
            boolean invalid = false;
            for (var diagnostic : diagnostics.getDiagnostics()) {
                if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                    invalid = true;
                    result.error(
                            path + ":" + diagnostic.getLineNumber(),
                            diagnostic.getMessage(java.util.Locale.ROOT));
                }
            }
            if (invalid) return null;
            SourcePositions positions = Trees.instance(task).getSourcePositions();
            Map<String, List<Decl>> declarations = new TreeMap<>();
            for (Tree tree : unit.getTypeDecls()) {
                if (tree instanceof ClassTree c)
                    indexClass(c, c.getSimpleName().toString(), unit, positions, declarations);
            }
            return new Parsed(source, unit, positions, declarations);
        } catch (IOException | RuntimeException e) {
            result.error(path, "cannot parse: " + e.getMessage());
            return null;
        }
    }

    static String methodKey(String owner, MethodTree method) {
        return owner
                + "#"
                + method.getName()
                + "("
                + method.getParameters().stream()
                        .map(p -> p.getType().toString())
                        .collect(Collectors.joining(","))
                + ")";
    }

    static String memberKey(String owner, Tree tree, int ordinal) {
        if (tree instanceof MethodTree m) return methodKey(owner, m);
        if (tree instanceof VariableTree v) return owner + "#field:" + v.getName();
        if (tree instanceof ClassTree c) return owner + "#type:" + c.getSimpleName();
        return owner + "#" + tree.getKind() + ":" + ordinal;
    }

    static void indexClass(
            ClassTree c,
            String owner,
            CompilationUnitTree unit,
            SourcePositions positions,
            Map<String, List<Decl>> index) {
        int ordinal = 0;
        for (Tree tree : c.getMembers()) {
            String key = memberKey(owner, tree, tree instanceof BlockTree ? ordinal++ : 0);
            boolean priv =
                    tree instanceof MethodTree m
                            ? m.getModifiers().getFlags().contains(Modifier.PRIVATE)
                            : tree instanceof VariableTree v
                                    ? v.getModifiers().getFlags().contains(Modifier.PRIVATE)
                                    : tree instanceof ClassTree nested
                                            && nested.getModifiers()
                                                    .getFlags()
                                                    .contains(Modifier.PRIVATE);
            long start = positions.getStartPosition(unit, tree);
            long line = start < 0 ? -1 : unit.getLineMap().getLineNumber(start);
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(new Decl(key, tree, line, priv));
            if (tree instanceof ClassTree nested) {
                indexClass(nested, owner + "." + nested.getSimpleName(), unit, positions, index);
            }
        }
    }

    static void compare(Map<String, Map<String, byte[]>> inputs, String policy, Result result) {
        Map<String, byte[]> reference = inputs.get("reference"),
                exercises = inputs.get("exercises");
        for (String side : SIDES) {
            if (inputs.get(side).isEmpty()) result.error(side, "empty source input");
        }
        Set<String> paths = new TreeSet<>(reference.keySet());
        paths.addAll(exercises.keySet());
        result.files = paths.size();
        Map<String, Map<String, Parsed>> parsed = new TreeMap<>();
        for (String side : SIDES) {
            Map<String, Parsed> units = new TreeMap<>();
            for (var entry : inputs.get(side).entrySet()) {
                if (!entry.getKey().endsWith(".java")) continue;
                try {
                    Parsed unit =
                            parse(side + "/" + entry.getKey(), decode(entry.getValue()), result);
                    if (unit != null) units.put(entry.getKey(), unit);
                } catch (CharacterCodingException e) {
                    result.error(side + "/" + entry.getKey(), "invalid UTF-8");
                }
            }
            parsed.put(side, units);
        }
        List<Rule> rules = readPolicy(policy, result);
        Map<String, Map<String, List<Span>>> edits = validateRules(rules, parsed, result);
        for (String path : paths) {
            if (!reference.containsKey(path) || !exercises.containsKey(path)) {
                result.drift(
                        path,
                        "file exists only in "
                                + (reference.containsKey(path) ? "reference" : "exercises"));
                continue;
            }
            if (!path.endsWith(".java")) {
                if (!Arrays.equals(reference.get(path), exercises.get(path)))
                    result.drift(path, "resource bytes differ");
                continue;
            }
            Parsed ref = parsed.get("reference").get(path), ex = parsed.get("exercises").get(path);
            if (ref == null || ex == null) continue;
            Parsed refProjection = project(path, "reference", ref, edits, result);
            Parsed exProjection = project(path, "exercises", ex, edits, result);
            if (refProjection != null && exProjection != null) {
                compareJava(path, refProjection, exProjection, ref, ex, result);
            }
        }
    }

    static Map<String, Map<String, List<Span>>> validateRules(
            List<Rule> rules, Map<String, Map<String, Parsed>> parsed, Result result) {
        Map<String, Map<String, List<Span>>> edits = new TreeMap<>();
        for (String side : SIDES) edits.put(side, new TreeMap<>());
        for (Rule rule : rules) {
            String key = "course/source-parity.tsv:" + rule.line;
            List<String> sides = rule.kind.equals("method-body") ? SIDES : List.of(rule.side);
            Map<String, Span> spans = new HashMap<>();
            boolean valid = true;
            for (String side : sides) {
                Parsed unit = parsed.get(side).get(rule.path);
                List<Decl> matches =
                        unit == null
                                ? List.of()
                                : unit.declarations.getOrDefault(rule.symbol, List.of());
                if (matches.size() != 1) {
                    result.error(
                            key,
                            side
                                    + "/"
                                    + rule.path
                                    + " "
                                    + rule.symbol
                                    + ": expected unique target, found "
                                    + matches.size());
                    valid = false;
                    continue;
                }
                Decl decl = matches.getFirst();
                Tree target = decl.tree;
                if (rule.kind.equals("method-body")) {
                    if (!(target instanceof MethodTree m) || m.getBody() == null) {
                        result.error(
                                key, rule.symbol + ": method-body requires a method with a body");
                        valid = false;
                        continue;
                    }
                    target = ((MethodTree) target).getBody();
                } else {
                    String opposite = side.equals("reference") ? "exercises" : "reference";
                    Parsed other = parsed.get(opposite).get(rule.path);
                    if (other == null) {
                        result.error(
                                key,
                                opposite + "/" + rule.path + ": missing parsed counterpart file");
                        valid = false;
                    } else if (other.declarations.containsKey(rule.symbol)) {
                        result.error(
                                key, rule.symbol + ": private-member now exists on both sides");
                        valid = false;
                    }
                    if (!decl.privateMember
                            || target instanceof MethodTree m
                                    && m.getName().contentEquals("<init>")) {
                        result.error(
                                key,
                                rule.symbol
                                        + ": private-member requires explicit private, non-constructor declaration");
                        valid = false;
                    }
                }
                long start = unit.positions.getStartPosition(unit.unit, target);
                long end = unit.positions.getEndPosition(unit.unit, target);
                if (rule.kind.equals("private-member") && target instanceof VariableTree) {
                    for (List<Decl> declarations : unit.declarations.values()) {
                        for (Decl sibling : declarations) {
                            if (sibling.tree == target || !(sibling.tree instanceof VariableTree))
                                continue;
                            long siblingStart =
                                    unit.positions.getStartPosition(unit.unit, sibling.tree);
                            long siblingEnd =
                                    unit.positions.getEndPosition(unit.unit, sibling.tree);
                            if (start < siblingEnd && siblingStart < end) {
                                result.error(
                                        key,
                                        rule.symbol
                                                + ": source range overlaps another field "
                                                + sibling.symbol
                                                + "; split multi-variable declarations before exempting a field");
                                valid = false;
                            }
                        }
                    }
                }
                if (start < 0 || end <= start || end > unit.source.length()) {
                    result.error(key, rule.symbol + ": invalid source range");
                    valid = false;
                } else {
                    spans.put(
                            side,
                            new Span(
                                    (int) start,
                                    (int) end,
                                    rule.kind.equals("method-body") ? "{}" : "",
                                    rule));
                }
            }
            if (valid) {
                for (String side : sides)
                    edits.get(side)
                            .computeIfAbsent(rule.path, p -> new ArrayList<>())
                            .add(spans.get(side));
                if (rule.kind.equals("method-body")) result.bodies++;
                else result.privateMembers++;
            }
        }
        for (String side : SIDES) {
            for (var entry : edits.get(side).entrySet()) {
                List<Span> spans = entry.getValue();
                spans.sort(Comparator.comparingInt(Span::start).thenComparingInt(Span::end));
                Set<Span> conflicts = new HashSet<>();
                for (int i = 0; i < spans.size(); i++) {
                    for (int j = i + 1;
                            j < spans.size() && spans.get(j).start < spans.get(i).end;
                            j++) {
                        Span a = spans.get(i), b = spans.get(j);
                        conflicts.add(a);
                        conflicts.add(b);
                        result.error(
                                "course/source-parity.tsv:" + b.rule.line,
                                side
                                        + "/"
                                        + entry.getKey()
                                        + ": overlapping exemptions "
                                        + a.rule.symbol
                                        + " / "
                                        + b.rule.symbol);
                    }
                }
                spans.removeAll(conflicts);
            }
        }
        return edits;
    }

    static Parsed project(
            String path,
            String side,
            Parsed parsed,
            Map<String, Map<String, List<Span>>> edits,
            Result result) {
        List<Span> spans = edits.get(side).getOrDefault(path, List.of());
        if (spans.isEmpty()) return parsed;
        StringBuilder source = new StringBuilder(parsed.source);
        spans.stream()
                .sorted(Comparator.comparingInt(Span::start).reversed())
                .forEach(span -> source.replace(span.start, span.end, span.replacement));
        return parse(side + "/" + path + " (projection)", source.toString(), result);
    }

    static List<String> retainedImports(CompilationUnitTree unit) {
        Set<String> identifiers = new HashSet<>();
        TreeScanner<Void, Void> scanner =
                new TreeScanner<>() {
                    @Override
                    public Void visitIdentifier(IdentifierTree tree, Void unused) {
                        identifiers.add(tree.getName().toString());
                        return null;
                    }
                };
        scanner.scan(unit.getPackage(), null);
        scanner.scan(unit.getTypeDecls(), null);
        scanner.scan(unit.getModule(), null);
        return unit.getImports().stream()
                .filter(
                        i -> {
                            String name = i.getQualifiedIdentifier().toString();
                            return name.endsWith(".*")
                                    || identifiers.contains(
                                            name.substring(name.lastIndexOf('.') + 1));
                        })
                .map(i -> (i.isStatic() ? "static " : "") + i.getQualifiedIdentifier())
                .sorted()
                .toList();
    }

    static String canonical(CompilationUnitTree unit) {
        return String.valueOf(unit.getPackage())
                + "\n"
                + unit.getTypeDecls()
                + "\n"
                + unit.getModule();
    }

    // Flat views localize structural differences without emitting entire answer source files.
    record View(String text, long line) {}

    static Map<String, View> views(Parsed projected, Parsed original) {
        Map<String, View> views = new TreeMap<>();
        for (Tree tree : projected.unit.getTypeDecls()) {
            if (tree instanceof ClassTree c)
                classViews(c, c.getSimpleName().toString(), projected, original, views);
        }
        return views;
    }

    static String classHeader(ClassTree c) {
        return c.getModifiers()
                + " "
                + c.getKind()
                + " "
                + c.getSimpleName()
                + " "
                + c.getTypeParameters()
                + " extends "
                + c.getExtendsClause()
                + " implements "
                + c.getImplementsClause()
                + " permits "
                + c.getPermitsClause();
    }

    static long originalLine(Parsed original, String symbol) {
        var matches = original.declarations.get(symbol);
        return matches == null || matches.isEmpty() ? -1 : matches.getFirst().line;
    }

    static void classViews(
            ClassTree c, String owner, Parsed projected, Parsed original, Map<String, View> views) {
        // A top-level type has no declaration-index entry; locate its original line by name.
        long line = -1;
        for (Tree tree : original.unit.getTypeDecls()) {
            if (tree instanceof ClassTree top && top.getSimpleName().contentEquals(owner)) {
                line =
                        original.unit
                                .getLineMap()
                                .getLineNumber(
                                        original.positions.getStartPosition(original.unit, tree));
            }
        }
        if (owner.contains(".")) {
            int dot = owner.lastIndexOf('.');
            line =
                    originalLine(
                            original,
                            owner.substring(0, dot) + "#type:" + owner.substring(dot + 1));
        }
        views.put(owner + "#declaration", new View(classHeader(c), line));
        List<String> order = new ArrayList<>();
        int ordinal = 0;
        Map<String, Integer> duplicates = new HashMap<>();
        for (Tree tree : c.getMembers()) {
            String key = memberKey(owner, tree, tree instanceof BlockTree ? ordinal++ : 0);
            order.add(key);
            int occurrence = duplicates.merge(key, 1, Integer::sum);
            if (tree instanceof ClassTree nested) {
                classViews(
                        nested, owner + "." + nested.getSimpleName(), projected, original, views);
            } else {
                views.put(
                        key + (occurrence == 1 ? "" : " [" + occurrence + "]"),
                        new View(tree.toString(), originalLine(original, key)));
            }
        }
        views.put(owner + "#member-order", new View(order.toString(), line));
    }

    static String snippet(String text) {
        return snippet(text, "");
    }

    static String snippet(String text, String counterpart) {
        // Preserve spaces inside literals; show the first changed region, not a shared prefix.
        String flat = text.replace('\n', ' ').replace('\r', ' ').strip();
        String other = counterpart.replace('\n', ' ').replace('\r', ' ').strip();
        int difference = 0;
        while (difference < flat.length()
                && difference < other.length()
                && flat.charAt(difference) == other.charAt(difference)) difference++;
        int start = Math.max(0, difference - 100);
        int end = Math.min(flat.length(), start + 420);
        return (start > 0 ? "..." : "")
                + flat.substring(start, end)
                + (end < flat.length() ? "..." : "");
    }

    static String describe(View view, View counterpart) {
        return view == null
                ? "<absent>"
                : "line "
                        + view.line
                        + ": "
                        + snippet(view.text, counterpart == null ? "" : counterpart.text);
    }

    static void compareJava(
            String path,
            Parsed ref,
            Parsed ex,
            Parsed originalRef,
            Parsed originalEx,
            Result result) {
        List<String> refImports = retainedImports(ref.unit), exImports = retainedImports(ex.unit);
        if (!refImports.equals(exImports)) {
            result.drift(
                    path + " #imports",
                    "reference "
                            + snippet(refImports.toString(), exImports.toString())
                            + " | exercises "
                            + snippet(exImports.toString(), refImports.toString()));
        }
        if (canonical(ref.unit).equals(canonical(ex.unit))) return;
        int before = result.problems.size();
        if (!String.valueOf(ref.unit.getPackage()).equals(String.valueOf(ex.unit.getPackage()))) {
            result.drift(
                    path + " #package",
                    "reference " + ref.unit.getPackage() + " | exercises " + ex.unit.getPackage());
        }
        Map<String, View> a = views(ref, originalRef), b = views(ex, originalEx);
        Set<String> symbols = new TreeSet<>(a.keySet());
        symbols.addAll(b.keySet());
        for (String symbol : symbols) {
            View av = a.get(symbol), bv = b.get(symbol);
            if (av == null || bv == null || !av.text.equals(bv.text)) {
                result.drift(
                        path + " " + symbol,
                        "reference " + describe(av, bv) + " | exercises " + describe(bv, av));
            }
        }
        List<String> refTypes =
                ref.unit.getTypeDecls().stream()
                        .map(
                                t ->
                                        t instanceof ClassTree c
                                                ? c.getSimpleName().toString()
                                                : t.getKind().toString())
                        .toList();
        List<String> exTypes =
                ex.unit.getTypeDecls().stream()
                        .map(
                                t ->
                                        t instanceof ClassTree c
                                                ? c.getSimpleName().toString()
                                                : t.getKind().toString())
                        .toList();
        if (!refTypes.equals(exTypes))
            result.drift(
                    path + " #type-order", "reference " + refTypes + " | exercises " + exTypes);
        if (!String.valueOf(ref.unit.getModule()).equals(String.valueOf(ex.unit.getModule()))) {
            result.drift(
                    path + " #module",
                    "reference "
                            + snippet(String.valueOf(ref.unit.getModule()))
                            + " | exercises "
                            + snippet(String.valueOf(ex.unit.getModule())));
        }
        if (result.problems.size() == before)
            result.drift(path + " #AST", "unlocalized compilation-unit structure differs");
    }

    static Map<String, byte[]> sources(String source) {
        return new TreeMap<>(Map.of("Demo.java", source.getBytes(StandardCharsets.UTF_8)));
    }

    static Result fixture(Map<String, byte[]> ref, Map<String, byte[]> ex, String policy) {
        Result result = new Result();
        compare(Map.of("reference", ref, "exercises", ex), policy, result);
        return result;
    }

    static void expect(
            String name,
            int expected,
            String location,
            Map<String, byte[]> ref,
            Map<String, byte[]> ex,
            String policy) {
        Map<String, byte[]> refBefore = new TreeMap<>(), exBefore = new TreeMap<>();
        ref.forEach((k, v) -> refBefore.put(k, v.clone()));
        ex.forEach((k, v) -> exBefore.put(k, v.clone()));
        Result result = fixture(ref, ex, policy);
        boolean untouched =
                ref.keySet().equals(refBefore.keySet())
                        && ex.keySet().equals(exBefore.keySet())
                        && ref.entrySet().stream()
                                .allMatch(
                                        e -> Arrays.equals(e.getValue(), refBefore.get(e.getKey())))
                        && ex.entrySet().stream()
                                .allMatch(
                                        e -> Arrays.equals(e.getValue(), exBefore.get(e.getKey())));
        if (result.exitCode() != expected
                || !untouched
                || !location.isEmpty() && !result.diagnostics().contains(location)) {
            throw new AssertionError(
                    name
                            + ": expected exit "
                            + expected
                            + " at "
                            + location
                            + ", observed "
                            + result.exitCode()
                            + ", input unchanged="
                            + untouched
                            + "\n"
                            + result.diagnostics());
        }
    }

    static int selfTest() {
        String bodyRule = "method-body\tboth\tDemo.java\tDemo#work(int)\tfixture hole\n";
        String privateRule =
                "private-member\treference\tDemo.java\tDemo#helper()\tfixture implementation\n";
        String policy = HEADER + "\n" + bodyRule + privateRule;
        String base =
                """
                package sample;
                import api.Token;
                import api.Tag;
                import java.util.List;
                class Demo {
                    static final int LIMIT = 16;
                    Token token;
                    Demo(int value) { if (value < 0) throw new IllegalArgumentException(); }
                    int supplied(int value) { return value + LIMIT; }
                    int work(int value) { return helper(); }
                    private int helper() { List<String> data = List.of("// { }"); return data.size(); }
                }
                """;
        String student =
                base.replace("return helper();", "throw new UnsupportedOperationException();")
                        .replace(
                                "    private int helper() { List<String> data = List.of(\"// { }\"); return data.size(); }\n",
                                "");
        try {
            expect("baseline", 0, "", sources(base), sources(student), policy);
            expect("identical unmasked", 0, "", sources(student), sources(student), HEADER + "\n");
            expect(
                    "body comments formatting imports",
                    0,
                    "",
                    sources(base),
                    sources(
                            student.replace(
                                            "import java.util.List;",
                                            "import unused.Implementation;")
                                    .replace("class Demo", "/** Different docs. */ class Demo")
                                    .replace(
                                            "throw new UnsupportedOperationException();",
                                            "String s = \"// { }\"; /* } */ return 0;")
                                    .replace("static final", "static\n final")),
                    policy);
            expect(
                    "text block body",
                    0,
                    "",
                    sources(base),
                    sources(
                            student.replace(
                                    "throw new UnsupportedOperationException();",
                                    "String text = \"\"\"\n // { }\n \"\"\"; return 1;")),
                    policy);
            expect(
                    "literal spelling",
                    0,
                    "",
                    sources(base),
                    sources(student.replace("LIMIT = 16", "LIMIT = 0x10")),
                    policy);
            String fieldRule =
                    "private-member\treference\tDemo.java\tDemo#field:extra\timplementation\n";
            expect(
                    "multi-field range cannot erase retained field",
                    2,
                    "overlaps another field",
                    sources(
                            base.replace(
                                    "Token token;",
                                    "Token token; private int shared = 1, extra = 2;")),
                    sources(
                            student.replace(
                                    "Token token;", "Token token; private int shared = 1;")),
                    policy + fieldRule);
            String[][] drifts = {
                {"constant", "LIMIT = 16", "LIMIT = 17", "Demo#field:LIMIT"},
                {
                    "provided body",
                    "return value + LIMIT",
                    "return value - LIMIT",
                    "Demo#supplied(int)"
                },
                {"constructor", "value < 0", "value <= 0", "Demo#<init>(int)"},
                {
                    "provided parameter name",
                    "supplied(int value) { return value",
                    "supplied(int renamed) { return renamed",
                    "Demo#supplied(int)"
                },
                {
                    "provided parameter type",
                    "supplied(int value)",
                    "supplied(long value)",
                    "Demo#supplied"
                },
                {
                    "provided throws",
                    "supplied(int value)",
                    "supplied(int value) throws Exception",
                    "Demo#supplied(int)"
                },
                {"provided annotation", "int supplied", "@Tag int supplied", "Demo#supplied(int)"},
                {"hole parameter name", "work(int value)", "work(int renamed)", "Demo#work(int)"},
                {
                    "hole throws",
                    "work(int value)",
                    "work(int value) throws Exception",
                    "Demo#work(int)"
                },
                {"hole annotation", "int work", "@Tag int work", "Demo#work(int)"},
                {"import binding", "api.Token", "other.Token", "#imports"},
                {"wildcard import", "import java.util.List;", "import java.util.*;", "#imports"}
            };
            for (String[] mutation : drifts)
                expect(
                        mutation[0],
                        1,
                        mutation[3],
                        sources(base),
                        sources(student.replace(mutation[1], mutation[2])),
                        policy);
            expect(
                    "undeclared helper",
                    1,
                    "Demo#extra()",
                    sources(base),
                    sources(
                            student.replace(
                                    "Token token;",
                                    "Token token; private int extra() { return 0; }")),
                    policy);
            var extraFile = sources(student);
            extraFile.put("Extra.java", "class Extra {}".getBytes(StandardCharsets.UTF_8));
            expect("extra file", 1, "Extra.java", sources(base), extraFile, policy);
            var resourceA = sources(base);
            resourceA.put("data.bin", new byte[] {0, 1});
            var resourceB = sources(student);
            resourceB.put("data.bin", new byte[] {0, 2});
            expect("resource bytes", 1, "data.bin", resourceA, resourceB, policy);
            String overloaded =
                    student.replace(
                            "Token token;", "Token token; int work(String value) { return 7; }");
            String refOverloaded =
                    base.replace(
                            "Token token;", "Token token; int work(String value) { return 7; }");
            expect(
                    "one overload exempt",
                    0,
                    "",
                    sources(refOverloaded),
                    sources(overloaded),
                    policy);
            expect(
                    "other overload drifts",
                    1,
                    "Demo#work(String)",
                    sources(refOverloaded),
                    sources(overloaded.replace("return 7", "return 8")),
                    policy);
            expect(
                    "duplicate rule",
                    2,
                    "duplicate exemption",
                    sources(base),
                    sources(student),
                    policy + bodyRule);
            expect(
                    "stale rule",
                    2,
                    "Demo#missing(int)",
                    sources(base),
                    sources(student),
                    policy.replace("Demo#work(int)", "Demo#missing(int)"));
            expect(
                    "private became public",
                    2,
                    "explicit private",
                    sources(base.replace("private int helper", "public int helper")),
                    sources(student),
                    policy);
            expect("private now common", 2, "both sides", sources(base), sources(base), policy);
            expect(
                    "nonunique target",
                    2,
                    "found 2",
                    sources(base),
                    sources(
                            student.replace(
                                    "Token token;",
                                    "Token token; int work(int other) { return 0; }")),
                    policy);
            expect(
                    "invalid path",
                    2,
                    "invalid relative",
                    sources(base),
                    sources(student),
                    policy.replace("Demo.java", "../Demo.java"));
            expect(
                    "empty column",
                    2,
                    "five nonempty",
                    sources(base),
                    sources(student),
                    policy.replace("fixture hole", ""));
            expect(
                    "unknown kind",
                    2,
                    "unknown kind/side",
                    sources(base),
                    sources(student),
                    policy.replace("method-body", "file"));
            expect("empty input", 2, "empty source input", Map.of(), Map.of(), HEADER + "\n");
            expect(
                    "syntax error",
                    2,
                    "exercises/Demo.java",
                    sources(base),
                    sources(student.replace("return value + LIMIT;", "return (;")),
                    policy);
            expect(
                    "invalid UTF8",
                    2,
                    "invalid UTF-8",
                    sources(base),
                    Map.of("Demo.java", new byte[] {(byte) 0xff}),
                    policy);
            expect(
                    "hole type invalidates key",
                    2,
                    "Demo#work(int)",
                    sources(base),
                    sources(student.replace("work(int value)", "work(long value)")),
                    policy);
            expect(
                    "hole name invalidates key",
                    2,
                    "Demo#work(int)",
                    sources(base),
                    sources(student.replace("work(int value)", "renamed(int value)")),
                    policy);
            expect(
                    "owner invalidates key",
                    2,
                    "Demo#work(int)",
                    sources(base),
                    sources(
                            student.replace("class Demo", "class Renamed")
                                    .replace("Demo(int value)", "Renamed(int value)")),
                    policy);
            var missingFile = new TreeMap<String, byte[]>();
            missingFile.put("Extra.java", "class Extra {}".getBytes(StandardCharsets.UTF_8));
            expect(
                    "policy-bound file deleted",
                    2,
                    "expected unique target",
                    sources(base),
                    missingFile,
                    policy);
            String constructorRule =
                    "private-member\treference\tDemo.java\tDemo#<init>()\tforbidden ctor\n";
            expect(
                    "private ctor cannot be removed",
                    2,
                    "non-constructor",
                    sources(base.replace("Token token;", "Token token; private Demo() {}")),
                    sources(student),
                    policy + constructorRule);
            String nestedRef =
                    base.replace(
                            "Token token;",
                            "Token token; private static class Helper { private int x; }");
            String nestedRule =
                    "private-member\treference\tDemo.java\tDemo#type:Helper\timplementation\n";
            expect(
                    "private nested type",
                    0,
                    "",
                    sources(nestedRef),
                    sources(student),
                    policy + nestedRule);
            expect(
                    "overlapping exclusions",
                    2,
                    "overlapping exemptions",
                    sources(nestedRef),
                    sources(student),
                    policy
                            + nestedRule
                            + "private-member\treference\tDemo.java\tDemo.Helper#field:x\tchild conflict\n");
            expect(
                    "member order",
                    1,
                    "#member-order",
                    sources(base),
                    sources(
                            student.replace(
                                    "static final int LIMIT = 16;\n    Token token;",
                                    "Token token;\n    static final int LIMIT = 16;")),
                    policy);
            expect(
                    "annotation import binding",
                    1,
                    "#imports",
                    sources(base.replace("int supplied", "@Tag int supplied")),
                    sources(
                            student.replace("int supplied", "@Tag int supplied")
                                    .replace("api.Tag", "other.Tag")),
                    policy);
            String staticRef =
                    base.replace("import api.Tag;", "import static api.Constants.VALUE;")
                            .replace("return value + LIMIT", "return value + VALUE");
            String staticEx =
                    student.replace("import api.Tag;", "import static other.Constants.VALUE;")
                            .replace("return value + LIMIT", "return value + VALUE");
            expect(
                    "static import binding",
                    1,
                    "#imports",
                    sources(staticRef),
                    sources(staticEx),
                    policy);
            String longPrefix = "value++; ".repeat(100);
            expect(
                    "diagnostic includes changed tail",
                    1,
                    "return value - LIMIT",
                    sources(
                            base.replace(
                                    "return value + LIMIT;", longPrefix + "return value + LIMIT;")),
                    sources(
                            student.replace(
                                    "return value + LIMIT;", longPrefix + "return value - LIMIT;")),
                    policy);
            System.out.println("source-parity: SELF-TEST OK");
            return 0;
        } catch (AssertionError | RuntimeException e) {
            System.err.println("source-parity: SELF-TEST FAILED " + e.getMessage());
            return 1;
        }
    }
}
