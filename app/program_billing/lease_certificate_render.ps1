param([string]$Folder)
$ErrorActionPreference='Stop'
$word=$null;$doc=$null
try {
$word=New-Object -ComObject Word.Application
$word.Visible=$false;$word.DisplayAlerts=0;$word.AutomationSecurity=3
$doc=$word.Documents.Open((Join-Path $Folder 'certificate.docx'),$false,$true)
$doc.ActiveWindow.View.Type=3;$doc.Repaginate()
Add-Type -AssemblyName System.Drawing
$pages=$doc.ActiveWindow.ActivePane.Pages
if($pages.Count -gt 30){throw 'Unsupported certificate page count'}
for($i=1;$i -le $pages.Count;$i++){
$emf=Join-Path $Folder ('page-'+$i+'.emf')
[IO.File]::WriteAllBytes($emf,[byte[]]$pages.Item($i).EnhMetaFileBits)
$meta=[Drawing.Imaging.Metafile]::new($emf)
$bitmap=[Drawing.Bitmap]::new(1654,2339)
$graphics=[Drawing.Graphics]::FromImage($bitmap)
$graphics.Clear([Drawing.Color]::White)
$graphics.DrawImage($meta,0,0,1654,2339)
$bitmap.Save((Join-Path $Folder ('page-'+$i+'.png')),[Drawing.Imaging.ImageFormat]::Png)
$graphics.Dispose();$bitmap.Dispose();$meta.Dispose()
}
} finally {
if($doc){$doc.Close(0)}
if($word){$word.Quit()}
}
