# Ini file buat unit testing
# Ya import2 lagi
import unittest
from unittest.mock import patch
from classes import QueryProcessor 

# Class testing
class TestQueryProcessor(unittest.TestCase):    
    # Testing pembuatan query processor
    def test_init(self):
        query = ""
        query_processor = QueryProcessor(query)
        self.assertEqual(query_processor.get_query(), "")
    
    # Testing setter (bruh)
    def test_set(self):
        query = ""
        query_processor = QueryProcessor(query)
        query_processor.set_query("SELECT id FROM users;")
        self.assertEqual(query_processor.get_query(), "SELECT id FROM users;")
    
    # Testing parsing (mungkin berubah)
    @patch('classes.parse')
    def test_parse_query(self, mock_parse):
        query = "SELECT id FROM users;"
        processor = QueryProcessor(query)
        expected_parse_tree = {
            "type": "Query",
            "children": [
                {"type": "Token.SELECT", "value": "SELECT"},
                {"type": "SelectList", "children": [
                    {"type": "Field", "children": [
                        {"type": "Token.ATTRIBUTE", "value": "id"}
                    ]}
                ]},
                {"type": "Token.FROM", "value": "FROM"},
                {"type": "FromList", "children": [
                    {"type": "TableReference", "children": [
                        {"type": "Token.TABLE", "value": "users"}
                    ]}
                ]},
                {"type": "Token.SEMICOLON", "value": ";"}
            ]
        }
        mock_parse.return_value = expected_parse_tree
        actual_parse_tree = processor.parse_query()
        mock_parse.assert_called_once_with(query)
        self.assertEqual(actual_parse_tree, expected_parse_tree)


if __name__ == '__main__':
    unittest.main()
