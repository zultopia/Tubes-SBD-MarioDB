from abc import ABC, abstractmethod

class QueryPlanOptimizer(ABC):
    @abstractmethod
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        pass