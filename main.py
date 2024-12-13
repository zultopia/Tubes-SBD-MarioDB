# semua komponen jadiin satu di sini
from QueryProcessor.classes import QueryProcessor, ExecutionResult
from QueryOptimizer.parse_tree import ParseTree

QueryProcessor = QueryProcessor()
while(True) :
    query_string = input('Please enter your query: ')
    if (query_string == "\\q") :
        break
    ExecutionResult = QueryProcessor.execute_query(query_string)
    print("Transaction_ID :", ExecutionResult.transaction_id)
    print("Timestamp :", ExecutionResult.timestamp)
    print("Message :", ExecutionResult.message)
    print("Rows :")
    # cek data ada atau kosong
    if ExecutionResult.data.data:
        # bentuk header dari keys
        headers = ExecutionResult.data.data[0].keys()
        # print row header
        print(f"{' | '.join(header.ljust(20) for header in headers)}")
        print("-" * (len(headers) * 20 + (len(headers) - 1) * 3))  # pemisah
        
        # print setiap record
        for record in ExecutionResult.data.data:
            print(f"{' | '.join(str(record.get(header, '')).ljust(20) for header in headers)}")
    else:
        print("No data available.")
    print("Query :", ExecutionResult.query)