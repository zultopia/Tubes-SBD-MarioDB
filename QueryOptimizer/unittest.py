from tests.test_rule1 import TestOptimizerRule1
from tests.test_rule2 import TestOptimizerRule2
        
if __name__ == '__main__':
    instance = TestOptimizerRule1()
    # For each method that starts with "test_"
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()
    
    instance = TestOptimizerRule2()
    # For each method that starts with "test_"
    for method_name in dir(instance):
        if method_name.startswith("test_"):
            method = getattr(instance, method_name)
            method()




