from pathlib import Path

OUT = Path("site/public/index.html")

html = """
<html>
<body>
<h1>EPL Analytics</h1>
<p>Data pipeline running.</p>
</body>
</html>
"""

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(html)
print("Site built")
