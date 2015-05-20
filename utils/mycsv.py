
import os

csv_output = ''

for i in range(1,4):
    file_txt= ''
    doc_path = 'cont-{}.{}'
    txt_path = doc_path.format(i, 'txt')
    docs = glob.glob(doc_path.format(i, '*'))
    file_txt = 'http://contratos.gastosabertos.org/files/{}'.format(txt_path) if txt_path in docs else ''
    print file_txt
    file_url = 'http://contratos.gastosabertos.org/files/{}'.format([d for d in docs if d != txt_path][0])
    csv_output += '{}, {}, {}\n'.format(i, file_url, file_txt)
    