with open('found_chars.txt', encoding='utf-8') as f:
    chars = f.read().splitlines()

output = []
for c in chars:
    if not c: continue
    try:
        decoded = c.encode('cp437').decode('utf-8')
        output.append(f"'{c}': '{decoded}',")
    except Exception as e:
        output.append(f"Failed for {c}: {e}")

with open('decoded_chars.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))
