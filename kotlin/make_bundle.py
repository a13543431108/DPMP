import os, zipfile

root = os.path.join("build", "staging-deploy")
out = os.path.join("build", "dpmp-bundle.zip")

with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            # 发布包不需要 maven-metadata
            if fn.startswith("maven-metadata.xml"):
                continue
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            z.write(full, rel)

print("bundle written:", out)
with zipfile.ZipFile(out) as z:
    for n in z.namelist()[:8]:
        print("  ", n)
    print("total entries:", len(z.namelist()))
