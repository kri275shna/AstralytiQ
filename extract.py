import re
with open('app.py', encoding='utf-8') as f:
    text = f.read()

matches = set(re.findall(r'[≡Γ][^\x00-\x7F]*', text))
with open('found_chars.txt', 'w', encoding='utf-8') as f:
    for m in matches:
        f.write(m + '\n')
