# Set your working directory where .mkv videos are stored
$folder = "D:\Users\yeshk\Documents\ait_platform\static\videos"  # 🔁 Replace with your actual folder
Set-Location $folder

# Loop through all .mkv files and convert to .mp4 with H.264 + AAC
Get-ChildItem -Filter *.mkv | ForEach-Object {
    $sourceFile = $_.FullName
    $outputFile = "$($_.DirectoryName)\$($_.BaseName).mp4"

    Write-Host "🎞️ Converting $($_.Name) to H.264 MP4..."

    ffmpeg -i "$sourceFile" -c:v libx264 -preset slow -crf 23 -c:a aac -b:a 128k "$outputFile"
}

Write-Host "`n✅ All videos converted to MP4 (H.264 + AAC)."
