import re

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

replacements = {
    '≡ƒôï': '📋',
    '≡ƒÆ░': '💰',
    '≡ƒæñ': '👤',
    '≡ƒÜ¬': '🚪',
    '≡ƒÜº': '🚧',
    '≡ƒº¡': '🧭',
    '≡ƒÅó': '🏢',
    '≡ƒôº': '📧',
    '≡ƒö┤': '🔴',
    '≡ƒöì': '🔍',
    '≡ƒöº': '🔧',
    # Handle variations manually based on visual similarities if they weren't caught
    '≡ƒÆ╝': '💼', # \U0001F4BC is brief case. F0 9F 92 BC -> ≡ƒÆ╝ 
}

for bad, good in replacements.items():
    content = content.replace(bad, good)

# Try to find any remaining broken characters dynamically
def fix_dynamic(match):
    try:
        return match.group(0).encode('cp437').decode('utf-8')
    except:
        return match.group(0)

content = re.sub(r'[≡Γ][^\x00-\x7F]*', fix_dynamic, content)
# Also try to fix any remaining characters starting with ≡ or Γ
# Let's just run an exhaustive search over the string and decode it
# Actually, the string replacements above are safer.

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Fixed additional emojis in app.py successfully!")
