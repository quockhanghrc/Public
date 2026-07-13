import io

path = r"c:\Users\vncpyy7h\Documents\vs_workspace\pub\Public\recommendations\MIND\src\model.py"
with io.open(path, "r", encoding="utf-8") as f:
    src = f.read()

old = (
    "            cat_vec = torch.cat([ce, se], dim=-1)               # (B, cat_total)\n"
    "            if self.category_mode == \"concat\":\n"
    "                cat_vec = cat_vec.unsqueeze(1).expand(-1, word_vecs.size(1), -1)\n"
    "                word_vecs = torch.cat([word_vecs, cat_vec], dim=-1)  # (B, seq, working_dim+cat)\n"
)
new = (
    "            cat_vec = torch.cat([ce, se], dim=-1)               # (B, cat_total)\n"
    "            # Both modes broadcast the category signal across the title sequence and\n"
    "            # concatenate it to word_vecs BEFORE the transformer blocks, because the\n"
    "            # blocks are built for the category-augmented dim (base_dim + cat_total).\n"
    "            cat_vec_seq = cat_vec.unsqueeze(1).expand(-1, word_vecs.size(1), -1)\n"
    "            word_vecs = torch.cat([word_vecs, cat_vec_seq], dim=-1)  # (B, seq, working_dim+cat)\n"
)

assert old in src, "OLD CNNNewsEncoder BLOCK NOT FOUND"
assert new not in src, "NEW CNNNewsEncoder BLOCK ALREADY PRESENT"
src = src.replace(old, new, 1)
with io.open(path, "w", encoding="utf-8") as f:
    f.write(src)
print("PATCHED model.py CNNNewsEncoder.forward cross-mode augmentation")
