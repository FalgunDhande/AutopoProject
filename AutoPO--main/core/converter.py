import os
import subprocess
from PIL import Image
import img2pdf

def convert_image_to_pdf(image_path, pdf_path):
    """Converts an image (JPG, PNG) to PDF."""
    try:
        with open(pdf_path, "wb") as f:
            f.write(img2pdf.convert(image_path))
        return True
    except Exception as e:
        print(f"Error converting image to PDF: {e}")
        return False

def convert_docx_to_pdf(docx_path, output_dir):
    """Converts a DOCX/DOC file to PDF using LibreOffice CLI."""
    try:
        # LibreOffice command: libreoffice --headless --convert-to pdf --outdir <dir> <file>
        subprocess.run([
            "libreoffice", "--headless", "--convert-to", "pdf", 
            "--outdir", output_dir, docx_path
        ], check=True)
        # The filename remains the same but extension changes to .pdf
        base_name = os.path.splitext(os.path.basename(docx_path))[0]
        pdf_path = os.path.join(output_dir, base_name + ".pdf")
        return pdf_path if os.path.exists(pdf_path) else None
    except Exception as e:
        print(f"Error converting DOCX to PDF: {e}")
        return None

def ensure_pdf(file_path):
    """
    Ensures the file is a PDF. If it's an image or DOCX, it converts it.
    Returns the path to the PDF version.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        return file_path
    
    directory = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    pdf_path = os.path.join(directory, base_name + ".pdf")

    if ext in [".jpg", ".jpeg", ".png"]:
        if convert_image_to_pdf(file_path, pdf_path):
            return pdf_path
    elif ext in [".docx", ".doc"]:
        return convert_docx_to_pdf(file_path, directory)
    
    return None
