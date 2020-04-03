# from tabula import read_pdf, convert_into
import tabula

tabula.convert_into(r'C:\Users\hariprasads\Downloads\ACSV0.6 (1)\ACSV0.6\ACSModel documentation.pdf',
                    r'C:\Users\hariprasads\Downloads\ACSV0.6 (1)\ACSV0.6\out.csv', output_format="csv", pages='all')
