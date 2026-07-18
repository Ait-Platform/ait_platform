with open('app/utils/queries.py', 'r', encoding='utf-8') as f:
    text = f.read()
    if 'BRIDGE_QUERY =' in text:
        start = text.find('BRIDGE_QUERY =')
        # Let's just print 1000 chars from start
        print(text[start:start+1500])
