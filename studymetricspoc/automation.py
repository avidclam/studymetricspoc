from nbclient import NotebookClient
import nbformat


def run_notebook(in_path, out_path):
    with open(in_path, encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)
    client = NotebookClient(nb, timeout=600)
    result = client.execute()
    with open(out_path, "w", encoding='utf-8') as f:
        nbformat.write(nb, f)