import sys
import subprocess
try:
    from PIL import Image
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Pillow"])
    from PIL import Image

def remove_white_corners(input_path, output_path):
    img = Image.open(input_path).convert("RGBA")
    data = img.load()
    width, height = img.size

    # We do a BFS flood fill from the corners
    # Target color is white-ish
    def is_whiteish(r, g, b, a):
        return r > 240 and g > 240 and b > 240 and a > 0

    visited = set()
    queue = [(0,0), (width-1, 0), (0, height-1), (width-1, height-1)]
    
    for sx, sy in queue:
        if (sx, sy) not in visited:
            r, g, b, a = data[sx, sy]
            if is_whiteish(r, g, b, a):
                q = [(sx, sy)]
                visited.add((sx, sy))
                while q:
                    x, y = q.pop(0)
                    data[x, y] = (255, 255, 255, 0) # Transparent
                    
                    # check neighbors
                    for dx, dy in [(-1,0), (1,0), (0,-1), (0,1)]:
                        nx, ny = x + dx, y + dy
                        if 0 <= nx < width and 0 <= ny < height:
                            if (nx, ny) not in visited:
                                nr, ng, nb, na = data[nx, ny]
                                if is_whiteish(nr, ng, nb, na):
                                    visited.add((nx, ny))
                                    q.append((nx, ny))
                                    
    img.save(output_path, "PNG")
    print(f"Saved transparent image to {output_path}")

if __name__ == "__main__":
    input_file = r"C:\Users\Sanjith\.gemini\antigravity\brain\62e36905-abc1-4639-958c-2951b58ade66\ait_logo_hex_refined_1782446194314.png"
    output_file = r"static\branding\ait_logo.png"
    remove_white_corners(input_file, output_file)
