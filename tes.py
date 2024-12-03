from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria

if __name__ == "__main__":
    com = FailureRecoveryManager()
    com.recover(RecoverCriteria(None, 101))
    
    print("last_log \n", com._wh_logs)