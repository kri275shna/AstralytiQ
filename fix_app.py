import re

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Fix Emojis
replacements = {
    "ΓÜí": "🚀",
    "≡ƒôè": "📊",
    "≡ƒöÆ": "🔐",
    "≡ƒñû": "🤖",
    "≡ƒöä": "🔄",
    "≡ƒôê": "📈",
    "≡ƒöù": "🔗",
    "≡ƒùä∩╕Å": "🗄️",
    "Γåù∩╕Å": "↗️",
    "Γåÿ∩╕Å": "↘️",
    "Γ£à": "✅",
    "ΓÜá∩╕Å": "⚠️",
    "ΓÜá": "⚠️",
    "Γ¥î": "❌",
    "Γ¡É": "⭐",
    "ΓÅ▒∩╕Å": "⏱️",
    "ΓÜÖ∩╕Å": "⚙️",
    "ΓÇó": "•",
    "≡ƒƒó": "🟢",
    "≡ƒÆ╛": "💾",
    "≡ƒöç": "🔔",
    "≡ƒôü": "📁"
}

for bad, good in replacements.items():
    content = content.replace(bad, good)

# 2. Fix indentation of st.markdown(..., unsafe_allow_html=True)
# The issue is that markdown blocks containing HTML have leading spaces which 
# makes Streamlit render them as code blocks.
# We will use regex to find st.markdown blocks and remove leading spaces inside the string.

def fix_markdown_indentation(match):
    before_string = match.group(1)
    markdown_str = match.group(2)
    after_string = match.group(3)
    
    # split lines, remove leading spaces, and rejoin
    lines = markdown_str.split('\n')
    # find minimum indentation of non-empty lines
    non_empty = [line for line in lines if line.strip()]
    if non_empty:
        min_indent = min(len(line) - len(line.lstrip()) for line in non_empty)
        # dedent
        dedented = [line[min_indent:] if len(line) >= min_indent and not line.isspace() else line for line in lines]
        return f"{before_string}{chr(10).join(dedented)}{after_string}"
    return match.group(0)

# Replace st.markdown("""...""", unsafe_allow_html=True)
# or st.markdown('''...''', unsafe_allow_html=True)

# Using regex to find the string part
pattern = re.compile(r'(st\.markdown\(\s*"""|st\.markdown\(\s*\'\'\')(.*?)(["\']{3}\s*,\s*unsafe_allow_html\s*=\s*True\s*\))', re.DOTALL)
content = pattern.sub(fix_markdown_indentation, content)

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Fixed app.py successfully!")
