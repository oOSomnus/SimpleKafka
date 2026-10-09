package io.simplekafka.support;

import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.reporting.legacy.xml.LegacyXmlReportGeneratingListener;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Runs course tests and reports results in the vocabulary of the course manifest. */
public final class CourseTestRunner {
    private static final String COURSE_PACKAGE = "io.simplekafka.course.";
    private static final String ORDER_PARAMETER = "junit.jupiter.testclass.order.default";
    private static final String PARALLEL_PARAMETER = "junit.jupiter.execution.parallel.enabled";

    private CourseTestRunner() {}

    public static void main(String[] args) {
        try {
            int exit = run(args);
            if (exit != 0) System.exit(exit);
        } catch (IllegalArgumentException exception) {
            System.err.println("Course test runner: " + exception.getMessage());
            if (exception.getCause() != null)
                System.err.println("  Cause: " + exception.getCause());
            System.exit(2);
        } catch (Throwable failure) {
            System.err.println("Course test runner failed during discovery or startup:");
            failure.printStackTrace(System.err);
            System.exit(2);
        }
    }

    private static int run(String[] args) throws IOException {
        if (args.length < 6) {
            throw new IllegalArgumentException(
                    "usage: CourseTestRunner <student|reference> <single|cumulative> <maxStep> "
                            + "<manifestPath> <reportsPath> <selectedClass>...");
        }
        String mode = args[0];
        if (!mode.equals("student") && !mode.equals("reference")) {
            throw new IllegalArgumentException("unknown mode: " + mode);
        }
        String scope = args[1];
        if (!scope.equals("single") && !scope.equals("cumulative")) {
            throw new IllegalArgumentException("unknown scope: " + scope);
        }
        int maxStep;
        try {
            maxStep = Integer.parseInt(args[2]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("maxStep must be an integer", exception);
        }

        Path manifestPath = Path.of(args[3]);
        Path reportsPath = Path.of(args[4]);
        Map<String, StepInfo> manifest;
        try {
            manifest = readManifest(manifestPath);
        } catch (IOException exception) {
            throw new IllegalArgumentException(
                    "cannot read course manifest: " + manifestPath, exception);
        }
        if (maxStep < 1 || maxStep > manifest.size()) {
            throw new IllegalArgumentException(
                    "maxStep must be from 1 to " + manifest.size() + ": " + args[2]);
        }
        List<String> selectedClasses = List.copyOf(Arrays.asList(args).subList(5, args.length));
        validateSelection(scope, maxStep, selectedClasses, manifest);

        TreeMap<Integer, StepStats> stepStats = new TreeMap<>();
        for (String className : selectedClasses) {
            StepInfo step = manifest.get(className);
            stepStats.put(step.step(), new StepStats(step));
        }
        List<DiscoverySelector> selectors =
                selectedClasses.stream()
                        .map(DiscoverySelectors::selectClass)
                        .map(selector -> (DiscoverySelector) selector)
                        .toList();
        LauncherDiscoveryRequest request =
                LauncherDiscoveryRequestBuilder.request()
                        .selectors(selectors)
                        .configurationParameter(
                                ORDER_PARAMETER, "org.junit.jupiter.api.ClassOrderer$ClassName")
                        .configurationParameter(PARALLEL_PARAMETER, "false")
                        .build();
        Launcher launcher = LauncherFactory.create();
        TestPlan plan = launcher.discover(request);
        Map<String, Integer> discoveredByClass = countDiscoveredTests(plan);
        for (String className : selectedClasses) {
            if (discoveredByClass.getOrDefault(className, 0) == 0) {
                throw new IllegalArgumentException(
                        "selected class has no discovered tests: " + className);
            }
            stepStats.get(manifest.get(className).step()).found = discoveredByClass.get(className);
        }

        printHeader(mode, scope, maxStep, reportsPath);
        CourseListener courseListener = new CourseListener(plan, manifest, stepStats);
        SummaryGeneratingListener summaryListener = new SummaryGeneratingListener();
        LegacyXmlReportGeneratingListener xmlListener =
                new LegacyXmlReportGeneratingListener(
                        reportsPath, new PrintWriter(System.err, true));
        long startedAt = System.nanoTime();
        launcher.execute(plan, courseListener, summaryListener, xmlListener);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        var summary = summaryListener.getSummary();
        courseListener.printStepSummaries();
        long failed = summary.getTestsFailedCount() + courseListener.failedContainers();
        System.out.printf(
                Locale.ROOT,
                "Results: found=%d started=%d passed=%d failed=%d aborted=%d skipped=%d | steps=%d | %d ms%n",
                summary.getTestsFoundCount(),
                summary.getTestsStartedCount(),
                summary.getTestsSucceededCount(),
                failed,
                summary.getTestsAbortedCount() + courseListener.abortedContainers(),
                summary.getTestsSkippedCount() + courseListener.skippedContainers(),
                stepStats.size(),
                elapsedMillis);

        boolean incomplete =
                summary.getTestsAbortedCount() > 0
                        || summary.getTestsSkippedCount() > 0
                        || courseListener.abortedContainers() > 0
                        || courseListener.skippedContainers() > 0;
        boolean unsuccessful = failed > 0 || incomplete;
        if (unsuccessful) {
            if (incomplete)
                System.out.println(
                        "Incomplete course check: aborted or skipped tests do not count as completion.");
            if (summary.getTotalFailureCount() > 0) {
                System.err.println("Failure details:");
                summary.printFailuresTo(new PrintWriter(System.err, true));
            }
            courseListener.printFailureNavigation(mode);
            return 1;
        }
        return 0;
    }

    private static Map<String, StepInfo> readManifest(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (lines.isEmpty()
                || !lines.get(0)
                        .equals(
                                "step\tchapter\ttitle_en\ttitle_zh\teditable_methods\ttest_class\tprerequisites")) {
            throw new IllegalArgumentException(
                    "manifest must have the seven-column course header: " + path);
        }
        Map<String, StepInfo> byClass = new HashMap<>();
        int expectedStep = 1;
        int previousChapter = 0;
        for (int index = 1; index < lines.size(); index++) {
            String[] columns = lines.get(index).split("\t", -1);
            if (columns.length != 7) {
                throw new IllegalArgumentException(
                        "manifest line " + (index + 1) + " must have seven columns");
            }
            int step;
            int chapter;
            try {
                step = Integer.parseInt(columns[0]);
                chapter = Integer.parseInt(columns[1]);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(
                        "manifest line " + (index + 1) + " has invalid step/chapter", exception);
            }
            boolean chapterSequenceValid =
                    expectedStep == 1
                            ? chapter == 1
                            : chapter == previousChapter || chapter == previousChapter + 1;
            if (step != expectedStep
                    || !chapterSequenceValid
                    || columns[2].isBlank()
                    || columns[3].isBlank()
                    || columns[4].isBlank()
                    || columns[5].isBlank()
                    || columns[6].isBlank()) {
                throw new IllegalArgumentException(
                        "manifest line " + (index + 1) + " has invalid course metadata");
            }
            String className = COURSE_PACKAGE + columns[5];
            StepInfo info =
                    new StepInfo(step, chapter, columns[2], columns[4], columns[6], className);
            if (byClass.putIfAbsent(className, info) != null) {
                throw new IllegalArgumentException(
                        "duplicate test class in manifest: " + className);
            }
            previousChapter = chapter;
            expectedStep++;
        }
        if (byClass.isEmpty()) {
            throw new IllegalArgumentException("manifest must contain at least one step");
        }
        return Map.copyOf(byClass);
    }

    private static void validateSelection(
            String scope,
            int maxStep,
            List<String> selectedClasses,
            Map<String, StepInfo> manifest) {
        if (selectedClasses.isEmpty())
            throw new IllegalArgumentException("at least one test class is required");
        Set<String> unique = new HashSet<>();
        List<StepInfo> selected = new ArrayList<>();
        for (String className : selectedClasses) {
            if (!unique.add(className))
                throw new IllegalArgumentException("duplicate selected class: " + className);
            StepInfo info = manifest.get(className);
            if (info == null)
                throw new IllegalArgumentException(
                        "selected class is not in the course manifest: " + className);
            if (info.step() > maxStep) {
                throw new IllegalArgumentException(
                        "selected class is outside maxStep " + maxStep + ": " + className);
            }
            selected.add(info);
        }
        selected.sort(Comparator.comparingInt(StepInfo::step));
        if (scope.equals("single")) {
            if (selected.size() != 1 || selected.get(0).step() != maxStep) {
                throw new IllegalArgumentException(
                        "single scope must select exactly Step " + maxStep);
            }
            return;
        }
        if (selected.size() != maxStep) {
            throw new IllegalArgumentException(
                    "cumulative scope must select every step from 1 through " + maxStep);
        }
        for (int index = 0; index < maxStep; index++) {
            if (selected.get(index).step() != index + 1) {
                throw new IllegalArgumentException(
                        "cumulative scope is missing Step " + (index + 1));
            }
        }
    }

    private static Map<String, Integer> countDiscoveredTests(TestPlan plan) {
        Map<String, Integer> counts = new HashMap<>();
        for (TestIdentifier root : plan.getRoots())
            collectDiscoveredTests(plan, root, null, counts);
        return counts;
    }

    private static void collectDiscoveredTests(
            TestPlan plan,
            TestIdentifier identifier,
            String inheritedClass,
            Map<String, Integer> counts) {
        String className = sourceClassName(identifier);
        if (className == null) className = inheritedClass;
        if (identifier.isTest() && className != null) counts.merge(className, 1, Integer::sum);
        for (TestIdentifier child : plan.getChildren(identifier)) {
            collectDiscoveredTests(plan, child, className, counts);
        }
    }

    private static String sourceClassName(TestIdentifier identifier) {
        return identifier
                .getSource()
                .map(
                        source -> {
                            if (source instanceof MethodSource methodSource)
                                return methodSource.getClassName();
                            if (source instanceof ClassSource classSource)
                                return classSource.getClassName();
                            return null;
                        })
                .orElse(null);
    }

    private static void printHeader(String mode, String scope, int maxStep, Path reportsPath) {
        System.out.println("simpleKafka course tests");
        if (scope.equals("single")) {
            System.out.printf(
                    Locale.ROOT, "Mode: %s | Scope: Step %02d (diagnostic only)%n", mode, maxStep);
        } else {
            System.out.printf(Locale.ROOT, "Mode: %s | Scope: Steps 01–%02d%n", mode, maxStep);
        }
        System.out.println("JUnit XML: " + reportsPath);
    }

    private record StepInfo(
            int step,
            int chapter,
            String title,
            String editableMethods,
            String prerequisites,
            String className) {}

    private static final class StepStats {
        private final StepInfo info;
        private int found;
        private int started;
        private int passed;
        private int failed;
        private int aborted;
        private int skipped;
        private int failedContainers;

        private StepStats(StepInfo info) {
            this.info = info;
        }
    }

    private record FailureLocation(int step, String location) {}

    private static final class CourseListener implements TestExecutionListener {
        private static final String RESET = "\u001B[0m";
        private static final String GREEN = "\u001B[32m";
        private static final String RED = "\u001B[31m";
        private static final String YELLOW = "\u001B[33m";
        private static final String CYAN = "\u001B[36m";

        private final TestPlan plan;
        private final Map<String, StepInfo> manifest;
        private final TreeMap<Integer, StepStats> steps;
        private final Map<String, Long> startedAt = new ConcurrentHashMap<>();
        private final List<FailureLocation> failures = new ArrayList<>();
        private final boolean color;
        private int failedContainers;
        private int abortedContainers;
        private int skippedContainers;

        private CourseListener(
                TestPlan plan, Map<String, StepInfo> manifest, TreeMap<Integer, StepStats> steps) {
            this.plan = plan;
            this.manifest = manifest;
            this.steps = steps;
            String term = System.getenv("TERM");
            this.color =
                    System.console() != null
                            && System.getenv("NO_COLOR") == null
                            && !"dumb".equals(term);
        }

        @Override
        public void executionStarted(TestIdentifier identifier) {
            if (identifier.isTest()) {
                startedAt.put(identifier.getUniqueId(), System.nanoTime());
                StepStats stats = statsFor(identifier);
                if (stats != null) stats.started++;
            }
            if (identifier.isContainer()) {
                StepInfo info = stepFor(identifier);
                if (info != null) {
                    System.out.println(
                            colorize(
                                    CYAN,
                                    String.format(
                                            Locale.ROOT,
                                            "\nStep %02d / Chapter %d — %s",
                                            info.step(),
                                            info.chapter(),
                                            info.title())));
                }
            }
        }

        @Override
        public void executionSkipped(TestIdentifier identifier, String reason) {
            StepStats stats = statsFor(identifier);
            if (identifier.isTest()) {
                if (stats != null) stats.skipped++;
                System.out.printf(
                        "%s %s (not run) — %s%n",
                        colorize(YELLOW, "[SKIP]"),
                        identifier.getDisplayName(),
                        reason == null || reason.isBlank() ? "no reason provided" : reason);
            } else {
                skippedContainers++;
                StepInfo info = stepFor(identifier);
                if (info != null)
                    System.out.printf(
                            "%s Step %02d container (not run) — %s%n",
                            colorize(YELLOW, "[SKIP]"),
                            info.step(),
                            reason == null || reason.isBlank() ? "no reason provided" : reason);
            }
        }

        @Override
        public void executionFinished(TestIdentifier identifier, TestExecutionResult result) {
            if (identifier.isTest()) {
                long start = startedAt.getOrDefault(identifier.getUniqueId(), System.nanoTime());
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                StepStats stats = statsFor(identifier);
                TestExecutionResult.Status status = result.getStatus();
                String label;
                String tone;
                switch (status) {
                    case SUCCESSFUL -> {
                        label = "PASS";
                        tone = GREEN;
                        if (stats != null) stats.passed++;
                    }
                    case ABORTED -> {
                        label = "ABORTED";
                        tone = YELLOW;
                        if (stats != null) stats.aborted++;
                    }
                    case FAILED -> {
                        label = "FAIL";
                        tone = RED;
                        if (stats != null) stats.failed++;
                        addFailure(identifier);
                    }
                    default -> throw new IllegalStateException("unknown test result: " + status);
                }
                System.out.printf(
                        Locale.ROOT,
                        "%s %s (%d ms)%s%n",
                        colorize(tone, "[" + label + "]"),
                        identifier.getDisplayName(),
                        elapsedMillis,
                        status == TestExecutionResult.Status.ABORTED
                                ? " — "
                                        + result.getThrowable()
                                                .map(CourseListener::message)
                                                .orElse("aborted")
                                : "");
            } else if (result.getStatus() == TestExecutionResult.Status.FAILED) {
                failedContainers++;
                StepStats stats = statsFor(identifier);
                if (stats != null) stats.failedContainers++;
                addFailure(identifier);
                System.out.printf(
                        "%s %s (container)%n",
                        colorize(RED, "[FAIL]"), identifier.getDisplayName());
            } else if (result.getStatus() == TestExecutionResult.Status.ABORTED) {
                abortedContainers++;
            }
        }

        private StepStats statsFor(TestIdentifier identifier) {
            StepInfo info = stepFor(identifier);
            return info == null ? null : steps.get(info.step());
        }

        private StepInfo stepFor(TestIdentifier identifier) {
            String className = sourceClassName(identifier);
            if (className == null) {
                TestIdentifier parent = plan.getParent(identifier).orElse(null);
                while (parent != null && className == null) {
                    className = sourceClassName(parent);
                    parent = plan.getParent(parent).orElse(null);
                }
            }
            return className == null ? null : manifest.get(className);
        }

        private void addFailure(TestIdentifier identifier) {
            StepInfo info = stepFor(identifier);
            String location =
                    identifier
                            .getSource()
                            .map(
                                    source -> {
                                        if (source instanceof MethodSource methodSource) {
                                            return methodSource.getClassName()
                                                    + "#"
                                                    + methodSource.getMethodName();
                                        }
                                        if (source instanceof ClassSource classSource)
                                            return classSource.getClassName();
                                        return identifier.getDisplayName();
                                    })
                            .orElse(identifier.getDisplayName());
            failures.add(
                    new FailureLocation(info == null ? Integer.MAX_VALUE : info.step(), location));
        }

        private void printStepSummaries() {
            for (StepStats stats : steps.values()) {
                System.out.printf(
                        Locale.ROOT,
                        "Step %02d summary: passed=%d failed=%d aborted=%d skipped=%d containersFailed=%d total=%d%n",
                        stats.info.step(),
                        stats.passed,
                        stats.failed + stats.failedContainers,
                        stats.aborted,
                        stats.skipped,
                        stats.failedContainers,
                        stats.found);
            }
        }

        private void printFailureNavigation(String mode) {
            if (failures.isEmpty()) return;
            Map<Integer, Set<String>> byStep = new TreeMap<>();
            for (FailureLocation failure : failures) {
                byStep.computeIfAbsent(failure.step(), ignored -> new java.util.TreeSet<>())
                        .add(failure.location());
            }
            System.err.println("Re-run and inspect:");
            for (Map.Entry<Integer, Set<String>> entry : byStep.entrySet()) {
                StepInfo info =
                        steps.containsKey(entry.getKey())
                                ? steps.get(entry.getKey()).info
                                : manifest.values().stream()
                                        .filter(row -> row.step() == entry.getKey())
                                        .findFirst()
                                        .orElse(null);
                if (info == null) {
                    System.err.println("  " + String.join(", ", entry.getValue()));
                    continue;
                }
                System.err.printf(
                        Locale.ROOT,
                        "  Step %02d: %s%n",
                        info.step(),
                        String.join(", ", entry.getValue()));
                System.err.println("    Editable: " + info.editableMethods());
                System.err.println("    Prerequisites: " + info.prerequisites());
                if (mode.equals("student")) {
                    System.err.printf(
                            Locale.ROOT,
                            "    ./gradlew :stepTest -Pstep=%d%n    ./gradlew :test -Pstep=%d%n",
                            info.step(),
                            info.step());
                } else {
                    System.err.printf(
                            Locale.ROOT, "    ./gradlew :referenceTest -Pstep=%d%n", info.step());
                }
            }
        }

        private int failedContainers() {
            return failedContainers;
        }

        private int abortedContainers() {
            return abortedContainers;
        }

        private int skippedContainers() {
            return skippedContainers;
        }

        private String colorize(String colorCode, String value) {
            return color ? colorCode + value + RESET : value;
        }

        private static String message(Throwable throwable) {
            String message = throwable.getMessage();
            return message == null || message.isBlank()
                    ? throwable.getClass().getSimpleName()
                    : message;
        }
    }
}
