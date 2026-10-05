from pathlib import Path

path = Path("include/raw/sample_video.mp4")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text("SIMULATED VIDEO CONTENT", encoding="utf-8")
print(path)
