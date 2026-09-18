"""Remove the filename prefix incorrectly included in ITK's wheel tag."""

from importlib.metadata import distribution

itk = distribution("itk")
for entry in itk.files:
    if str(entry).endswith(".dist-info/WHEEL"):
        wheel = itk.locate_file(entry)
        contents = wheel.read_text()
        corrected = contents.replace(f"Tag: itk-{itk.version}-", "Tag: ")
        if corrected != contents:
            wheel.write_text(corrected)
