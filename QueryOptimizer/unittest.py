from QueryOptimizer.tests.test_rule1 import TestOptimizerRule1
from QueryOptimizer.tests.test_rule2 import TestOptimizerRule2
from QueryOptimizer.tests.test_rule4 import TestOptimizerRule4
from QueryOptimizer.tests.test_rule5 import TestOptimizerRule5
from QueryOptimizer.tests.test_rule6 import TestOptimizerRule6
from QueryOptimizer.tests.test_rule7 import TestOptimizerRule7
from QueryOptimizer.tests.test_rule8 import TestOptimizerRule8
from QueryOptimizer.tests.test_query_plan_tree import TestQueryPlanGeneration
        
if __name__ == '__main__':
    print("=============================================================")

    instance = TestOptimizerRule1()
    print("Testing for rule 1")
    # For each method that starts with "test_"
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()
    print("\n\n=============================================================")
    
    instance = TestOptimizerRule2()
    # For each method that starts with "test_"
    print("Testing for rule 2")
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()
    print("\n\n=============================================================")


    instance = TestOptimizerRule4()
    # For each method that starts with "test_"
    print("Testing for rule 4")
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()
    print("\n\n=============================================================")

    instance = TestOptimizerRule5()
    # For each method that starts with "test_"
    print("Testing for rule 5")
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()

    # print("\n\n=============================================================")

    # instance = TestOptimizerRule6()
    # print("Testing for rule 6")
    # # For each method that starts with "test_"
    # for method_name in dir(instance):
    #     if method_name.startswith("test_"):
    #         method = getattr(instance, method_name)
    #         method()

    # print("\n\n=============================================================")

    instance = TestOptimizerRule7()
    print("Testing for rule 7")
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()
    print("\n\n=============================================================")
    
    instance = TestOptimizerRule8()
    print("Testing for rule 8")
    # For each method that starts with "test_"
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()

    print("\n\n=============================================================")
    instance = TestQueryPlanGeneration()
    print("Testing for query plan generation")
    # For each method that starts with "test_"
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()

    




