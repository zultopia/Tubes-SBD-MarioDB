# semua komponen jadiin satu di sini
from QueryProcessor.classes import QueryProcessor

QueryProcessor = QueryProcessor()

while True:
    try:
        query_string = input('Please enter your query: ').strip()
        if query_string == "\\q":
            print("Exiting...")
            break
        elif not query_string:
            print("Empty query, please enter a valid SQL command or '\\q' to quit.")
            continue
        else:
            QueryProcessor.execute_query(query_string)
    except Exception as e:
        print(f"Error: {e}")