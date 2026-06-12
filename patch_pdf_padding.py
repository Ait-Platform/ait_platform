import re

def main():
    filepath = r'templates/school_home/certificate.html'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Add padding: 0; to the inner td that has the background color
    content = content.replace('style="background-color: #0033a1; height: 10px;"', 'style="background-color: #0033a1; height: 10px; padding: 0;"')
    
    # Add padding: 0; to the empty remaining td
    content = content.replace('></td><td></td></tr>', '></td><td style="padding: 0;"></td></tr>')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    main()
