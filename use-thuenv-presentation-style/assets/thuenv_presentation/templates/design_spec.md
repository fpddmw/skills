---
deck_id: thuenv_presentation
kind: deck
category: brand
summary: Branded presentation system for Tsinghua University School of Environment teaching, research, leadership, project, and external-facing presentations.
keywords: [thuenv, tsinghua, environment, education, research]
primary_color: "#92278F"
canvas_format: ppt169
canvas_width: 1280
canvas_height: 720
canvas_viewbox: "0 0 1280 720"
source_canvas_width: 1280
source_canvas_height: 720
source_viewbox: "0 0 1280 720"
replication_mode: fidelity
native_structure_mode: structured
page_count: 9
placeholders:
  03e_content_section_title: ["{{SECTION_NAME}}", "{{PAGE_NUM}}", "{{PAGE_TITLE}}", "{{CONTENT_AREA}}"]
---

# Tsinghua University School of Environment Presentation — Design Specification

## I. Template Overview

| Application context | Definition |
|---|---|
| Recurring presentation family | Teaching, research, project progress, school operations, leadership briefings, academic exchange, and external presentations produced by Tsinghua University School of Environment. |
| Intended audiences and outcomes | Internal and external faculty, students, leaders, partners, and professional audiences; enable clear presentation of evidence, analysis, proposals, progress, and conclusions under a consistent school identity. |
| Delivery and reading assumptions | Primarily presenter-led 16:9 projection, with enough hierarchy and source space to remain useful as a leave-behind document. |
| Representative narrative/page roles | Branded cover, chapter emphasis, evidence and data pages, comparison, process and system diagrams, image-supported explanation, progress, action, and closing transition. |

The deck is light-first and formal. White information fields are framed by Tsinghua purple, restrained gold accents, School of Environment identity marks, and campus photography. The visual system should feel institutional and credible without becoming ceremonial or visually heavy.

## II. Color Scheme

| Role | Color | Usage |
|---|---|---|
| Primary | `#92278F` | Footer rule, emphasis, section markers, key relationships |
| Secondary field | `#F5EEF7` | Light purple evidence or grouping field |
| Accent | `#BA9E43` | Sparse highlight for key distinctions and conclusions |
| Background | `#FFFFFF` | Main information field |
| Primary text | `#1F1F1F` | Titles and body text |
| Secondary text | `#595959` | Captions, notes, supporting explanations |
| Divider | `#D8D8D8` | Light rules and placeholder boundaries |

Purple carries identity and emphasis; gold remains a secondary accent and should not become a competing dominant color. Data pages may add semantic colors only when the content requires them.

## III. Typography

- Chinese heading and body export face: `Microsoft YaHei`.
- English companion face: `Arial`.
- Titles are concise and weight-led; body content remains neutral and highly legible.
- Source title hierarchy and generous content-area proportions are retained as visual references, while each downstream project owns its final confirmed type scale.

## IV. Signature Design Elements

- A School of Environment campus-image band with the school identity mark defines standard content pages.
- A 23 px Tsinghua-purple footer rule closes standard content and evidence layouts.
- The cover uses the source campus photograph, purple overlay, school identity mark, centered title, and restrained gold support band.
- Chapter emphasis has two source-derived forms: a bottom panoramic campus image with a purple rule, and a left vertical campus image with a purple divider.
- Content layouts keep large white working fields so the downstream project can build editable diagrams, charts, matrices, and evidence compositions without fighting the identity layer.
- The four content layouts differ by reusable slot topology: one flexible field, two equal fields, a four-zone data grid, and a picture/text split.
- The section-aware content layout places the running section name in the upper-right banner and standardizes the body heading as a fixed purple rule plus editable page number and page title slots.

## V. Page Roster

| SVG | Master / Layout | Prototype role and structural capacity |
|---|---|---|
| `01_cover.svg` | `thuenv-master` / `thuenv-cover` | Full branded cover with fixed campus imagery and school mark; title, subtitle, author, and date slots. |
| `02a_chapter_bottom_photo.svg` | `thuenv-master` / `thuenv-chapter-bottom` | Breathing chapter or strategic-judgment page with centered title/description and a bottom panoramic campus image. |
| `02b_chapter_left_photo.svg` | `thuenv-master` / `thuenv-chapter-left` | Chapter or emphasis page with a left vertical campus image, purple divider, and right-side title/description. |
| `03a_content_flexible.svg` | `thuenv-master` / `thuenv-content-flex` | Standard header/footer identity with one large flexible object region for diagrams, charts, timelines, and mixed content. |
| `03b_content_two_col.svg` | `thuenv-master` / `thuenv-content-two-col` | Standard header/footer identity with two equal object regions for comparisons and parallel evidence. |
| `03c_content_data_grid.svg` | `thuenv-master` / `thuenv-content-data-grid` | Standard header/footer identity with four object regions for metrics, evidence blocks, or a 2×2 analytical structure. |
| `03d_content_image_split.svg` | `thuenv-master` / `thuenv-content-image-split` | Standard header/footer identity with a left picture slot and a larger right object region for explanation or structured evidence. |
| `03e_content_section_title.svg` | `thuenv-master` / `thuenv-content-section-title` | Standard content page with editable upper-right section name, independent page number and body-title slots, a fixed purple title rule, and one large flexible content region. |
| `04_ending.svg` | `thuenv-master` / `thuenv-ending` | Closing, decision ask, or transition page with centered message and bottom panoramic campus image. |

## VI. Assets

| File | Source use |
|---|---|
| `image9.png` | Standard content-page campus banner layer |
| `image10.png` | Standard content-page purple campus overlay layer |
| `image11.png` | Tsinghua University and School of Environment identity mark |
| `image12.jpeg` | Cover campus photograph |
| `image13.png` | Cover purple overlay |
| `image14.png` | Cover identity mark |
| `image15.png` | Bottom panoramic campus photograph |
| `image16.jpeg` | Left vertical campus photograph and picture-slot preview |

## VII. Placeholder Overrides

The `03e_content_section_title` variant extends the standard content vocabulary with `{{SECTION_NAME}}` and an independently editable `{{PAGE_NUM}}`. This keeps the running section label and page-within-section numbering consistent while preserving `{{PAGE_TITLE}}` and `{{CONTENT_AREA}}` as normal content-page slots.
