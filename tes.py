from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria

def main():
    recover = FailureRecoveryManager()
    
    # recover.recover(RecoverCriteria(None, 103))
    recover.recover_system_crash()
    
    print("last stage: ", recover._wa_logs)
    exit()
    
if __name__ == "__main__":
    main()