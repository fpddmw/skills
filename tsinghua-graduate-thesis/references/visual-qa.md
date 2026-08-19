# PDF 视觉 QA

机器检查不能证明页面看起来正确。每次最终编译后都要渲染代表页并实际查看。

## 预检

```bash
SKILL_DIR='/absolute/path/to/tsinghua-graduate-thesis'
pdfinfo thesis.pdf
pdffonts thesis.pdf
render_dir=$(mktemp -d "${TMPDIR:-/tmp}/tsinghua-thesis-render.XXXXXX")
node "$SKILL_DIR/scripts/render-pdf.mjs" \
  --input thesis.pdf \
  --output-dir "$render_dir" \
  --first-page 1 \
  --last-page 1 \
  --probe-page 1 \
  --dpi 150 \
  --json
```

要求 A4 页面、预期页数与空白页策略、字体嵌入、书签/元数据和非加密状态符合本次提交要求。若工具支持，先用文本/书签搜索定位页面，再渲染；不要依赖文本抽取判断版式。

`--probe-page` 必须是已知含可见文字或图形的页面。对不连续的代表页分别运行命令，并为每轮使用新的空输出目录。需要锁定 renderer 时可重复传入 `--candidate /absolute/path/to/pdftoppm`；未传时脚本依次检查 `PDFTOPPM`、`PATH` 和常见 Homebrew 位置。

## Renderer 健康门禁

不要直接调用裸 `pdftoppm` 做最终审查。某些被移动后的 Poppler 运行时找不到 `poppler-data`，会在退出码仍为 0 时生成空白图片。helper 会用真实 PDF 页面探测候选 renderer，并把以下错误视为运行环境故障：

- `Missing language pack`
- `Unknown font tag`
- `No font in show` / `No font in show/space`
- `Couldn't find ... CMap file`
- 已知非空探测页没有足够可见像素

只有 JSON 结果为 `status: "ok"` 才能查看其 `outputs` 并继续视觉结论。若所有候选均失败，结果为 `renderer_environment_error`：停止本轮视觉判定，修复/提供健康 renderer 后重跑，并把论文状态记为“未验证”。不得据空白输出报告“论文缺字”，也不得为迎合故障截图修改 LaTeX 源。

## 必看页面角色

从 [格式要求清单](format-requirements.json) 的 `visualReview.pageRoles` 选择：中文封面、英文封面、授权页、中文摘要、目录、正文第一页、包含图表公式的复杂页、参考文献、声明页和最后一项材料。缺少某类页面时记录“不适用”，不要用别的页面冒充。

每页检查：

- 裁切、页面尺寸、边距、居中和左右页位置
- 中文/西文字体、字号层级、行距、字距与粗细
- 页眉、页脚、页码制式和重置点
- 长标题换行、目录悬挂、图表题注、公式编号和参考文献悬挂缩进
- 扫描页清晰度、方向、裁切、页眉页码叠加与签名真实性
- 缺字、乱码、黑块、重叠、溢出、孤行、意外空页或低分辨率图片

## 迭代规则

发现缺陷后回到 LaTeX 源或配置修复，重新完整编译，再渲染受影响页以及相邻页。不要直接编辑最终 PDF 来掩盖源文件问题。

只有 helper 确认 renderer 健康，且最新一轮代表页检查没有可见缺陷，才能将视觉状态标记为“已验证”。如最终字体环境、院系专用规则、真实扫描页或健康 renderer 缺失，只能标记“有条件通过”或“未验证”。
