# semua komponen jadiin satu di sini
from QueryProcessor.classes import QueryProcessor, ExecutionResult
from QueryOptimizer.parse_tree import ParseTree

QueryProcessor = QueryProcessor()

query_type = str(input("Mau melakukan natural join atau tidak? (Y/N)"))
if (query_type == "Y"):
    query_string_student = str(input("Masukkan query untuk tabel Student (StudentID harus ada): "))
    query_string_course = str(input("Masukkan query untuk tabel Course (CourseID harus ada): "))
    ExecutionResult_student = QueryProcessor.execute_query(query_string_student)
    ExecutionResult_attends = QueryProcessor.execute_query("SELECT StudentID, CourseID FROM Attends;")
    ExecutionResult_course = QueryProcessor.execute_query(query_string_course)

    students = ExecutionResult_student.data.data
    attends = ExecutionResult_attends.data.data
    courses = ExecutionResult_course.data.data

    result = []

    for attend in attends:
        student = next(student for student in students if student["StudentID"] == attend["StudentID"])
        course = next(course for course in courses if course["CourseID"] == attend["CourseID"])
        
        joined_data = {**student, **course}
        result.append(joined_data)

    for record in result:
        print(record)

elif (query_type == "N"):
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