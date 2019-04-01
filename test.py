from unittest import TestCase, main

import process_efet_carc

df = pd.read_excel(file, sheet_name=0, header=None)

class Testes(TestCase):

    
    
    def test_data_doc_new(self):
        self.assertEqual(process_efet_carc.get_data_doc_new(df), '04/02/2019')

if __name__ == '__main__':
    main()

