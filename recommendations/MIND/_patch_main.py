import re, io

path = r"c:\Users\vncpyy7h\Documents\vs_workspace\pub\Public\recommendations\MIND\main.py"
with io.open(path, "r", encoding="utf-8") as f:
    src = f.read()

old = (
    "    working_dim = args.bottleneck_dim if args.bottleneck_dim else args.embed_dim\n"
    "    new_nh = _fit_heads(args.num_heads, working_dim)\n"
    "    new_unh = _fit_heads(args.user_num_heads, working_dim)\n"
)
new = (
    "    working_dim = args.bottleneck_dim if args.bottleneck_dim else args.embed_dim\n"
    "    # When category_mode is enabled, the category+subcategory signal is concatenated\n"
    "    # to the working dim before the transformer blocks, so heads must divide the\n"
    "    # AUGMENTED dim (not just the word/working dim).\n"
    "    if args.category_mode != \"none\":\n"
    "        working_dim = working_dim + args.cat_embed_dim + args.subcat_embed_dim\n"
    "    new_nh = _fit_heads(args.num_heads, working_dim)\n"
    "    new_unh = _fit_heads(args.user_num_heads, working_dim)\n"
)

assert old in src, "OLD BLOCK NOT FOUND"
assert new not in src, "NEW BLOCK ALREADY PRESENT"
src = src.replace(old, new, 1)
with io.open(path, "w", encoding="utf-8") as f:
    f.write(src)
print("PATCHED main.py head-fit block")
