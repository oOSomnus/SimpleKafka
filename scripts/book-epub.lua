local function fail(message)
  error('book EPUB reader: ' .. message, 0)
end

local function trim(value)
  return (value:gsub('^%s*(.-)%s*$', '%1'))
end

local function read_file(path)
  local file, err = io.open(path, 'rb')
  if not file then
    fail(string.format('cannot read %s: %s', path, err or 'unknown error'))
  end
  local text = file:read('*a')
  file:close()
  return text
end

local function normalized_input(path)
  if path:sub(1, 1) == '/' then
    fail('absolute TeX input is not allowed: ' .. path)
  end
  local parts = {}
  for part in path:gmatch('[^/]+') do
    if part == '..' then
      fail('TeX input escapes docs/book: ' .. path)
    elseif part ~= '.' and part ~= '' then
      parts[#parts + 1] = part
    end
  end
  if #parts == 0 then
    fail('empty TeX input path')
  end
  return table.concat(parts, '/')
end

local function expand_inputs(path, active, chain)
  path = normalized_input(path)
  if active[path] then
    chain[#chain + 1] = path
    fail('recursive TeX input: ' .. table.concat(chain, ' -> '))
  end
  active[path] = true
  chain[#chain + 1] = path
  local text = read_file(path)
  local expanded = {}
  for line in (text .. '\n'):gmatch('(.-)\n') do
    line = line:gsub('\r$', '')
    local input = line:match('^%s*\\input%s*{%s*([^}]-)%s*}%s*$')
    if input then
      expanded[#expanded + 1] = expand_inputs(input, active, chain)
    else
      expanded[#expanded + 1] = line
    end
  end
  chain[#chain] = nil
  active[path] = nil
  return table.concat(expanded, '\n')
end

local function replace_literal(text, needle, replacement)
  local start = 1
  local pieces = {}
  while true do
    local first, last = text:find(needle, start, true)
    if not first then
      pieces[#pieces + 1] = text:sub(start)
      break
    end
    pieces[#pieces + 1] = text:sub(start, first - 1)
    pieces[#pieces + 1] = replacement
    start = last + 1
  end
  return table.concat(pieces)
end

local function adapt_latex(text)
  text = replace_literal(text, '\\texttt{\\detokenize{#1}}', '\\texttt{#1}')
  text = replace_literal(text, '\\texttt{\\detokenize{recoveryRead=true}}', '\\texttt{recoveryRead=true}')
  text = replace_literal(text,
    '\\begin{description}[style=nextline,leftmargin=3.2em,labelwidth=2.7em]',
    '\\begin{description}')
  return text
end

local function count_literal(text, needle)
  local count, position = 0, 1
  while true do
    local first, last = text:find(needle, position, true)
    if not first then
      return count
    end
    count = count + 1
    position = last + 1
  end
end

local function compile_diagrams(diagrams)
  if #diagrams == 0 then
    return {}
  end
  local tectonic = os.getenv('BOOK_EPUB_TECTONIC')
  local pdftoppm = os.getenv('BOOK_EPUB_PDFTOPPM')
  local bundle = os.getenv('BOOK_EPUB_BUNDLE')
  local work = os.getenv('BOOK_EPUB_WORK')
  local cache = os.getenv('TECTONIC_CACHE_DIR')
  local offline = os.getenv('BOOK_OFFLINE')
  if not tectonic or tectonic == '' or not pdftoppm or pdftoppm == '' or
      not bundle or bundle == '' or not work or work == '' or not cache or cache == '' then
    fail('missing internal renderer configuration (BOOK_EPUB_TECTONIC, BOOK_EPUB_PDFTOPPM, BOOK_EPUB_BUNDLE, BOOK_EPUB_WORK, TECTONIC_CACHE_DIR)')
  end
  if offline ~= '0' and offline ~= '1' and offline ~= nil then
    fail('BOOK_OFFLINE must be 0 or 1')
  end

  local preamble = read_file('preamble.tex')
  local tex = {
    '\\documentclass[11pt]{article}',
    preamble,
    '\\usepackage[active,tightpage,xetex]{preview}',
    '\\setlength{\\PreviewBorder}{2pt}',
    '\\begin{document}'
  }
  for _, diagram in ipairs(diagrams) do
    tex[#tex + 1] = '\\begin{preview}'
    tex[#tex + 1] = diagram
    tex[#tex + 1] = '\\end{preview}'
  end
  tex[#tex + 1] = '\\end{document}'
  local tex_path = work .. '/diagrams.tex'
  local tex_file, tex_err = io.open(tex_path, 'wb')
  if not tex_file then
    fail(string.format('cannot write %s: %s', tex_path, tex_err or 'unknown error'))
  end
  tex_file:write(table.concat(tex, '\n'), '\n')
  tex_file:close()

  local args = {
    '-X', 'compile', tex_path,
    '--outdir', work,
    '--keep-logs',
    '--untrusted',
    '--bundle', bundle
  }
  if offline == '1' then
    args[#args + 1] = '--only-cached'
  end
  pandoc.pipe(tectonic, args, '')
  local pdf = work .. '/diagrams.pdf'
  local pdf_file = io.open(pdf, 'rb')
  if not pdf_file then
    fail('Tectonic did not create ' .. pdf)
  end
  local pdf_header = pdf_file:read(5)
  pdf_file:close()
  if pdf_header ~= '%PDF-' then
    fail('Tectonic created an invalid diagram PDF: ' .. pdf)
  end

  local paths = {}
  for index = 1, #diagrams do
    local prefix = work .. '/diagram-' .. index
    pandoc.pipe(pdftoppm, {
      '-f', tostring(index), '-l', tostring(index), '-singlefile',
      '-r', '180', '-png', pdf, prefix
    }, '')
    local image = prefix .. '.png'
    local image_file = io.open(image, 'rb')
    if not image_file then
      fail(string.format('diagram page %d produced no PNG: %s', index, image))
    end
    local first_byte = image_file:read(1)
    image_file:close()
    if not first_byte then
      fail(string.format('diagram page %d produced an empty PNG: %s', index, image))
    end
    paths[index] = image
  end
  return paths
end

local function convert_tikz(text)
  local starts = count_literal(text, '\\begin{tikzpicture}')
  local ends = count_literal(text, '\\end{tikzpicture}')
  if starts ~= ends then
    fail(string.format('unmatched TikZ environment (begin=%d, end=%d)', starts, ends))
  end
  local diagrams, replacements = {}, 0
  local body = text:gsub('\\begin{tikzpicture}.-\\end{tikzpicture}', function(diagram)
    replacements = replacements + 1
    diagrams[#diagrams + 1] = diagram
    return '\\includegraphics{BOOK_EPUB_DIAGRAM_' .. replacements .. '}'
  end)
  if replacements ~= starts then
    fail(string.format('could not isolate all TikZ environments (found %d, replaced %d)', starts, replacements))
  end
  local paths = compile_diagrams(diagrams)
  if #paths ~= replacements then
    fail(string.format('rendered %d of %d TikZ diagrams', #paths, replacements))
  end
  for index, path in ipairs(paths) do
    body = replace_literal(body, 'BOOK_EPUB_DIAGRAM_' .. index, path)
  end
  if body:find('BOOK_EPUB_DIAGRAM_', 1, true) then
    fail('an internal diagram placeholder remains after rendering')
  end
  return body
end

local function strip_known_layout(text)
  local commands = {
    'large', 'small', 'normalsize', 'noindent', 'centering',
    'bgroup', 'egroup', 'arraybackslash', 'frontmatter', 'mainmatter',
    'appendix', 'backmatter', 'maketitle', 'tableofcontents'
  }
  for _, command in ipairs(commands) do
    text = text:gsub('\\' .. command .. '%f[^%a]', '')
  end
  text = text:gsub('\\hypersetup%s*{%s*pageanchor%s*=%s*false%s*}', '')
  text = text:gsub('\\hypersetup%s*{%s*pageanchor%s*=%s*true%s*}', '')
  text = text:gsub('\\setlength%s*{%s*\\tabcolsep%s*}%s*{%s*3pt%s*}', '')
  text = text:gsub('\\addcontentsline%s*{%s*toc%s*}%s*{%s*chapter%s*}%s*{%s*Glossary%s*}', '')
  text = text:gsub('\\addcontentsline%s*{%s*toc%s*}%s*{%s*chapter%s*}%s*{%s*术语索引%s*}', '')
  return text
end

local function unsupported_raw(raw)
  local text = trim(raw.text):gsub('[\r\n]+', ' ')
  if #text > 180 then
    text = text:sub(1, 177) .. '...'
  end
  fail(string.format('unsupported raw %s content: %q', raw.format, text))
end

local function convert_raw_inline(raw)
  if raw.format ~= 'latex' then
    unsupported_raw(raw)
  end
  local text = raw.text
  if not text:find('\\newline', 1, true) then
    if trim(strip_known_layout(text)) == '' then
      return {}
    end
    unsupported_raw(raw)
  end
  local result, position = {}, 1
  while true do
    local first, last = text:find('\\newline', position, true)
    if not first then
      local remainder = strip_known_layout(text:sub(position))
      if trim(remainder) ~= '' then
        unsupported_raw(pandoc.RawInline('latex', remainder))
      end
      break
    end
    local before = strip_known_layout(text:sub(position, first - 1))
    if trim(before) ~= '' then
      unsupported_raw(pandoc.RawInline('latex', before))
    end
    result[#result + 1] = pandoc.LineBreak()
    position = last + 1
  end
  return result
end

local function convert_raw_block(raw)
  if raw.format ~= 'latex' then
    unsupported_raw(raw)
  end
  if trim(strip_known_layout(raw.text)) == '' then
    return {}
  end
  unsupported_raw(raw)
end

local function add_figure_alt_text(figure)
  local caption = figure.caption
  local alt
  if caption and caption.long then
    for _, block in ipairs(caption.long) do
      if block.t == 'Para' or block.t == 'Plain' then
        alt = block.content
        break
      end
    end
  end
  if alt and #alt > 0 then
    figure.content = figure.content:walk({
      Image = function(image)
        image.caption = alt
        return image
      end
    })
  end
  return figure
end

function Reader(sources, _options)
  if #sources ~= 1 then
    fail('expected one master TeX source')
  end
  local source = sources[1]
  local expanded = expand_inputs(source.name, {}, {})
  local text = adapt_latex(expanded)
  text = convert_tikz(text)

  local document = pandoc.read(text, 'latex+raw_tex')
  document = document:walk({
    RawInline = convert_raw_inline,
    RawBlock = convert_raw_block,
    Figure = add_figure_alt_text
  })

  if document.meta.date then
    document.meta.subtitle = document.meta.date
    document.meta.date = nil
  end
  return document
end
