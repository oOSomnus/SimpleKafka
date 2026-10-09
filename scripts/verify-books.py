#!/usr/bin/env python3
"""Validate the distributed textbook artifacts as readers consume them."""

import argparse
import posixpath
import re
import sys
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET


CONTAINER_NS = "urn:oasis:names:tc:opendocument:xmlns:container"
OPF_NS = "http://www.idpf.org/2007/opf"
DC_NS = "http://purl.org/dc/elements/1.1/"
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
STEP_IDS = tuple("step:{}".format(number) for number in range(1, 32))

BOOKS = {
    "en": {
        "stem": "simpleKafka",
        "language": "en",
        "title": "Understanding simpleKafka from Scratch",
        "author": "simpleKafka teaching project",
        "preface": "Before you begin",
        "appendix": "Formats, Error Codes, and Interface Index",
        "glossary": "Glossary",
        "chapters": (
            "Course Map, Tools, and How to Learn",
            "Records and Single-Segment Persistence",
            "Segments, Indexing, Retention, and Truncation",
            "Partitions and a Broker on a Real Network",
            "Producing, Consuming, Offsets, and Redelivery",
            "Consumer Groups, Rebalancing, and Commit Fencing",
            "Replication, ISR, and High Watermarks",
            "Election, Log Repair, and Replica Readmission",
            "Consistency Across Producers, Brokers, and Consumers",
        ),
        "hint_terms": ("Level 1", "Level 2", "Level 3"),
    },
    "zh": {
        "stem": "simpleKafka-zh",
        "language": "zh-CN",
        "title": "从零理解 simpleKafka",
        "author": "simpleKafka 教学项目",
        "preface": "写在开始之前",
        "appendix": "格式、错误码与接口索引",
        "glossary": "术语索引",
        "chapters": (
            "课程地图、工具与学习方式",
            "记录与单段持久化",
            "分段、索引、保留与截断",
            "分区与真实网络 broker",
            "生产消费、位点与重复投递",
            "消费组、重平衡与提交围栏",
            "拉取副本、ISR、高水位与确认",
            "epoch围栏、干净选举、分叉修复与重新入组",
            "生产者、broker 与消费者的一致性",
        ),
        "hint_terms": ("一级", "二级", "三级"),
    },
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def normalized(text):
    return re.sub(r"\s+", " ", text or "").strip()


def local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


def element_text(element):
    return normalized(" ".join(element.itertext()))


def parse_xml(data, label):
    try:
        return ET.fromstring(data)
    except ET.ParseError as error:
        raise ValueError("{}: invalid XML: {}".format(label, error)) from error


def archive_target(source_path, href):
    parsed = urlsplit(href)
    if parsed.scheme or parsed.netloc:
        return None, unquote(parsed.fragment)
    target_path = unquote(parsed.path)
    if target_path.startswith("/"):
        target = posixpath.normpath(target_path.lstrip("/"))
    elif target_path:
        target = posixpath.normpath(
            posixpath.join(posixpath.dirname(source_path), target_path)
        )
    else:
        target = source_path
    if target == ".." or target.startswith("../") or target.startswith("/"):
        raise ValueError("archive path escapes the EPUB root: {}".format(href))
    return target, unquote(parsed.fragment)


def validate_pdf(path, label):
    try:
        with path.open("rb") as pdf:
            header = pdf.read(5)
            pdf.seek(0, 2)
            size = pdf.tell()
            pdf.seek(max(0, size - 1024))
            trailer = pdf.read()
    except OSError as error:
        raise ValueError("{}: cannot read PDF: {}".format(label, error)) from error
    require(header == b"%PDF-", "{}: missing PDF header".format(label))
    require(b"%%EOF" in trailer, "{}: missing PDF EOF marker".format(label))


def validate_epub(path, label, expected):
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as error:
        raise ValueError("{}: cannot open EPUB ZIP: {}".format(label, error)) from error

    with archive:
        members = archive.namelist()
        infos = archive.infolist()
        require(bool(infos), "{}: EPUB archive is empty".format(label))
        require(infos[0].filename == "mimetype", "{}: first ZIP item is not mimetype".format(label))
        require(
            infos[0].compress_type == zipfile.ZIP_STORED,
            "{}: mimetype entry must be uncompressed".format(label),
        )
        try:
            mimetype = archive.read("mimetype")
            damaged = archive.testzip()
        except (OSError, RuntimeError, zipfile.BadZipFile) as error:
            raise ValueError("{}: EPUB ZIP integrity check failed: {}".format(label, error)) from error
        require(mimetype == b"application/epub+zip", "{}: invalid EPUB mimetype".format(label))
        require(damaged is None, "{}: damaged EPUB ZIP member {}".format(label, damaged))
        member_set = set(members)

        try:
            container = parse_xml(archive.read("META-INF/container.xml"), label + "/container.xml")
        except KeyError as error:
            raise ValueError("{}: missing META-INF/container.xml".format(label)) from error
        rootfile = container.find(".//{{{}}}rootfile".format(CONTAINER_NS))
        require(rootfile is not None, "{}: container.xml has no rootfile".format(label))
        opf_path = posixpath.normpath(rootfile.get("full-path", ""))
        require(opf_path in member_set, "{}: missing OPF resource {}".format(label, opf_path))
        package = parse_xml(archive.read(opf_path), label + "/" + opf_path)
        require(
            (package.get("version") or "").startswith("3."),
            "{}: OPF version is not EPUB 3".format(label),
        )

        metadata = package.find("{{{}}}metadata".format(OPF_NS))
        require(metadata is not None, "{}: OPF has no metadata".format(label))
        titles = [element_text(item) for item in metadata.findall("{{{}}}title".format(DC_NS))]
        authors = [element_text(item) for item in metadata.findall("{{{}}}creator".format(DC_NS))]
        languages = [element_text(item) for item in metadata.findall("{{{}}}language".format(DC_NS))]
        require(
            any(expected["title"] in title for title in titles),
            "{}: OPF title does not contain {!r}".format(label, expected["title"]),
        )
        require(
            expected["author"] in authors,
            "{}: OPF author {!r} is missing".format(label, expected["author"]),
        )
        require(
            expected["language"] in languages,
            "{}: OPF language must include {!r}; found {}".format(
                label, expected["language"], languages
            ),
        )

        manifest = package.find("{{{}}}manifest".format(OPF_NS))
        spine = package.find("{{{}}}spine".format(OPF_NS))
        require(manifest is not None, "{}: OPF has no manifest".format(label))
        require(spine is not None, "{}: OPF has no spine".format(label))
        items = {}
        resources = {}
        nav_paths = []
        for item in manifest.findall("{{{}}}item".format(OPF_NS)):
            item_id = item.get("id")
            href = item.get("href")
            media_type = item.get("media-type")
            require(item_id and href and media_type, "{}: malformed manifest item".format(label))
            require(item_id not in items, "{}: duplicate manifest id {}".format(label, item_id))
            target, fragment = archive_target(opf_path, href)
            require(target is not None, "{}: external manifest resource {}".format(label, href))
            require(target in member_set, "{}: missing manifest resource {}".format(label, target))
            items[item_id] = (target, media_type)
            resources[target] = media_type
            if "nav" in (item.get("properties") or "").split():
                nav_paths.append(target)
        require(bool(nav_paths), "{}: OPF has no navigation document".format(label))

        spine_paths = []
        for reference in spine.findall("{{{}}}itemref".format(OPF_NS)):
            item_id = reference.get("idref")
            require(item_id in items, "{}: spine references missing manifest id {}".format(label, item_id))
            target, media_type = items[item_id]
            require(
                media_type == "application/xhtml+xml",
                "{}: spine item {} is not XHTML".format(label, target),
            )
            spine_paths.append(target)
        require(bool(spine_paths), "{}: OPF spine is empty".format(label))

        xhtml_documents = {}
        for resource_path, media_type in resources.items():
            if media_type != "application/xhtml+xml":
                continue
            document = parse_xml(archive.read(resource_path), label + "/" + resource_path)
            ids = set()
            for element in document.iter():
                for attribute, value in element.attrib.items():
                    if attribute == "id" or attribute.endswith("}id"):
                        ids.add(value)
            xhtml_documents[resource_path] = (document, ids)

        require(bool(xhtml_documents), "{}: no XHTML resources".format(label))
        for source_path, (document, _) in xhtml_documents.items():
            for element in document.iter():
                for attribute in ("href", "src"):
                    href = element.get(attribute)
                    if not href:
                        continue
                    target, fragment = archive_target(source_path, href)
                    if target is None:
                        continue
                    require(
                        target in member_set,
                        "{}: {} in {} points to missing archive file {}".format(
                            label, attribute, source_path, target
                        ),
                    )
                    if fragment and target in xhtml_documents:
                        require(
                            fragment in xhtml_documents[target][1],
                            "{}: link in {} points to missing id {}#{}".format(
                                label, source_path, target, fragment
                            ),
                        )

        all_ids = {}
        all_text = []
        step_headings = {}
        code_text = set()
        figures = []
        images = []
        h1_texts = set()
        for resource_path, (document, ids) in xhtml_documents.items():
            for identifier in ids:
                all_ids.setdefault(identifier, []).append(resource_path)
            all_text.extend(document.itertext())
            for element in document.iter():
                tag = local_name(element.tag)
                text = element_text(element)
                if tag == "h1":
                    h1_texts.add(text)
                if tag == "section" and element.get("id", "").startswith("step:"):
                    step_headings[element.get("id")] = (resource_path, element)
                if tag == "code":
                    code_text.add(text)
                if tag == "figure":
                    figures.append(element)
                if tag == "img":
                    images.append((resource_path, element))

        for step_id in STEP_IDS:
            require(step_id in all_ids, "{}: missing step id {}".format(label, step_id))
            require(len(all_ids[step_id]) == 1, "{}: duplicate step id {}".format(label, step_id))
            require(step_id in step_headings, "{}: step id {} is not a step section".format(label, step_id))

        ordered_steps = []
        for resource_path, (document, _) in xhtml_documents.items():
            elements = list(document.iter())
            for index, element in enumerate(elements):
                if local_name(element.tag) == "section" and element.get("id", "").startswith("step:"):
                    ordered_steps.append((resource_path, elements, index, element.get("id")))
        ordered_steps.sort(key=lambda entry: (entry[0], entry[2]))
        terms = expected["hint_terms"]
        for resource_path, elements, index, step_id in ordered_steps:
            next_index = len(elements)
            for candidate_index in range(index + 1, len(elements)):
                candidate = elements[candidate_index]
                if (
                    local_name(candidate.tag) == "section"
                    and candidate.get("id", "").startswith("step:")
                ):
                    next_index = candidate_index
                    break
            found_terms = [
                element_text(candidate)
                for candidate in elements[index + 1 : next_index]
                if local_name(candidate.tag) == "dt"
            ]
            require(
                all(found_terms.count(term) == 1 for term in terms)
                and len(found_terms) == len(terms),
                "{}: {} in {} must retain exactly three hint levels {}; found {}".format(
                    label, step_id, resource_path, terms, found_terms
                ),
            )
        require(len(ordered_steps) == 31, "{}: expected 31 step headings, found {}".format(label, len(ordered_steps)))

        missing_chapters = [title for title in expected["chapters"] if title not in h1_texts]
        require(
            not missing_chapters,
            "{}: missing one or more of the nine chapter headings: {}".format(
                label, missing_chapters
            ),
        )
        require(expected["preface"] in h1_texts, "{}: missing preface {}".format(label, expected["preface"]))
        require(expected["appendix"] in h1_texts, "{}: missing appendix {}".format(label, expected["appendix"]))
        require(expected["glossary"] in h1_texts, "{}: missing glossary {}".format(label, expected["glossary"]))

        require(len(figures) == 6, "{}: expected six figures, found {}".format(label, len(figures)))
        require(len(images) == 6, "{}: expected six embedded images, found {}".format(label, len(images)))
        for resource_path, image in images:
            source = image.get("src")
            alt = normalized(image.get("alt"))
            require(bool(alt), "{}: image in {} has no caption-based alt text".format(label, resource_path))
            target, _ = archive_target(resource_path, source or "")
            require(target is not None and target in member_set, "{}: missing image resource {}".format(label, source))
            data = archive.read(target)
            require(data.startswith(PNG_SIGNATURE), "{}: image is not a readable PNG: {}".format(label, target))
        for figure in figures:
            require(
                any(local_name(child.tag) == "figcaption" for child in figure.iter()),
                "{}: figure has no visible caption".format(label),
            )

        visible_text = normalized(" ".join(all_text))
        require("CORRUPT_RECORD" in visible_text, "{}: missing CORRUPT_RECORD contract".format(label))
        require("GroupCoordinator" in visible_text, "{}: missing GroupCoordinator contract reference".format(label))
        required_commands = (
            "./gradlew :stepTest -Pstep=1",
            "./gradlew :test -Pstep=31",
        )
        for command in required_commands:
            require(command in code_text, "{}: missing code command {!r}".format(label, command))
        required_steprefs = {"step:1", "step:4", "step:14", "step:22", "step:25"}
        linked_steprefs = set()
        for document, _ in xhtml_documents.values():
            for element in document.iter():
                if local_name(element.tag) != "a":
                    continue
                href = element.get("href", "")
                _, fragment = archive_target("", href)
                if fragment in required_steprefs:
                    linked_steprefs.add(fragment)
        require(
            linked_steprefs == required_steprefs,
            "{}: missing internal step references {}; found {}".format(
                label, sorted(required_steprefs), sorted(linked_steprefs)
            ),
        )


def validate(root):
    require(root.is_dir(), "book output root is not a directory: {}".format(root))
    for language, expected in BOOKS.items():
        base = root / language / expected["stem"]
        pdf_path = base.with_suffix(".pdf")
        epub_path = base.with_suffix(".epub")
        label = "{} ({})".format(language, base)
        validate_pdf(pdf_path, label)
        validate_epub(epub_path, label, expected)
        print("{}: PDF and EPUB structure/content OK".format(language))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("book_output_root", type=Path)
    args = parser.parse_args()
    try:
        validate(args.book_output_root)
    except (OSError, ValueError, KeyError, zipfile.BadZipFile, RuntimeError) as error:
        print("book validation: {}".format(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
