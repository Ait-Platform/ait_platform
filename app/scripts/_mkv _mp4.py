import subprocess
import os

def convert_mkv_to_mp4(input_file, output_dir='ConvertedVideos'):
    if not input_file.endswith('.mkv'):
        raise ValueError("Input file must be an MKV file.")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Build output file path
    base_name = os.path.basename(input_file).replace('.mkv', '.mp4')
    output_file = os.path.join(output_dir, base_name)

    # Check if output file already exists
    if os.path.exists(output_file):
        print(f"⚠️ Skipping: '{output_file}' already exists.")
        return

    # FFmpeg command with re-encoding
    command = [
        'ffmpeg',
        '-i', input_file,
        '-c:v', 'libx264',
        '-c:a', 'aac',
        '-strict', 'experimental',
        output_file
    ]

    try:
        subprocess.run(command, check=True)
        print(f"✅ Conversion successful: {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during conversion: {e}")

# Example usage
convert_mkv_to_mp4('C:/Videos/example_video.mkv')