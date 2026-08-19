# ThuThesis 工作流

## 适用边界

ThuThesis 是社区维护的清华论文 LaTeX 实现。它负责大量版式默认值，但不替代当前院系要求、研究生院指南文字或人工内容核对。

## 初始化或接管项目

1. 记录 ThuThesis 的发布版本、来源 URL 和完整包哈希。使用完整发布包，不从第三方项目复制单个 `.cls`。
2. 保持模板类、参考文献样式和发布资源原样；用户没有明确要求时，不修改 `thuthesis.cls`、`.bst`、`.bbx` 或 `.cbx` 来“修视觉”。
3. 新项目从发布版示例复制并重命名主文件。已有项目先识别真正的 root `.tex`、模板版本、构建入口和本地修改，再决定是否升级。
4. 升级模板前保存可编译基线和 PDF；逐项阅读跨版本更新，尤其是封面、授权/声明页、目录、字体和弃用选项。

## 低摩擦画像映射

核对 `\documentclass` 与 `\thusetup`，至少覆盖：

- `degree = master | doctor`
- `degree-type = academic | professional`
- `language`、特殊项目的 `style-override`
- 打印版或电子版 `output`
- 中英文标题、学位类别、培养单位、学科/专业领域
- 作者、导师、副导师/联合导师、日期和涉密设置

先运行 `scripts/requirements.mjs` 生成当前画像清单，再配置项目。不要猜测学位门类、专业领域英文名或导师角色；缺失信息保持显式占位并列为阻塞项。

常用映射：

- `degree`、`degree-type`、`style-override`、`fontset` 属于 `\documentclass` 选项。
- `output`、`language`、中英文标题/学位类别、院系、学科/领域、作者、导师、日期、密级属于 `\thusetup`。
- `\maketitle` 生成封面；`committee`、`abstract`/`abstract*`、`denotation`、`acknowledgements`、`resume`/`achievements` 生成对应部分。
- `\copyrightpage`、`\statement` 等只能调用一次；真实扫描页通过其 `file` 参数替换。
- 目录/清单使用 `\tableofcontents`、`\listoffigures`、`\listoftables` 或 `\listoffiguresandtables`；附录由 `\appendix` 开始。

## 结构与内容

按论文画像核对封面、委员会/评阅人页、授权页、中英文摘要、目录、图表清单、符号表、正文、参考文献、附录、致谢、声明、个人简历与成果、评语和答辩决议等部分。对条件性页面只在来源或用户信息支持时启用。

签字扫描页必须来自用户提供的真实文件。插入前后都检查 PDF 页尺寸、方向、裁切和页眉页码；不要生成签名图像。

## 字体、数学与文献

- 终版优先 `fontset=windows`，以取得中易宋体/黑体/仿宋、Times New Roman 和 Arial；替代字体只能形成条件性验证。
- 中文论文数学默认 `math-style=GB`，默认数学字体 XITS Math。数学常数/函数和微分号用正体，向量/矩阵/张量用粗斜体。
- 图、表、公式连接符统一选 `.` 或 `-`，通过 `figure-number-separator`、`table-number-separator`、`equation-number-separator` 显式设置。
- 参考文献只选择一套后端：BibTeX + natbib，或 BibLaTeX + biber。顺序编码制用 `thuthesis-numeric`，著者-出版年制用 `thuthesis-author-year`。

## 安全编译

1. 先阅读构建文件。用户提供的 `latexmkrc`、Makefile 或 TeX 源可能启用 `-shell-escape` 或执行任意命令。
2. 对未审计来源，使用独立工作目录并默认禁用 shell escape。安全基线示例：

```bash
latexmk -norc -pdfxe -interaction=nonstopmode -halt-on-error -file-line-error thesis.tex
```

3. 只有已确认依赖确实需要、命令内容已审计且用户授权时才启用 shell escape。不要因为发布包自带配置就自动执行它。
4. 使用与最终提交一致的 TeX 引擎、模板版本和字体环境。终版优先使用学校要求对应的 Windows 中文字体；替代字库只能算条件性验证。
5. 构建到交叉引用稳定。BibTeX 附录若有独立参考文献，还需编译相应的 `*-appendix-*.aux`；BibLaTeX 使用 biber。

## 编译检查

- 编译到交叉引用和目录稳定，且没有未定义引用/文献。
- 审查缺失字体、字体替代、重复标签、过宽/过窄盒子和 PDF inclusion 警告。
- 检查 A4 页面、嵌入字体、书签、PDF 元数据、页数、加密状态和空白页策略。
- 渲染抽查封面、摘要、目录、正文首章、长标题、图表公式、参考文献、附录、声明和个人简历等代表页面。
- 不为消除单个 warning 直接改模板类；先定位是内容、宏包冲突、字体还是模板版本问题。

## 交付

交付完整可复现源目录、最终 PDF、画像清单、构建命令、模板/字体/文献环境和未解决警告摘要。不要提交缓存、辅助文件、密钥、真实签名源图或无关个人数据。
