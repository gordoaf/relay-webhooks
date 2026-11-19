"""Build the standalone demo from the same assets used by the server console."""

from pathlib import Path
import shutil

root = Path(__file__).resolve().parents[1]
source = root / 'src' / 'relay' / 'static'
output = root / 'dist'
assets = output / 'assets'
assets.mkdir(parents=True, exist_ok=True)
for item in source.iterdir():
    if item.is_file():
        shutil.copy2(item, output / item.name if item.name == 'index.html' else assets / item.name)
(assets / 'demo-config.js').write_text('window.RELAY_DEMO = true;\n', encoding='utf-8')
(output / '.nojekyll').touch()
print(f'Demo built at {output}')
