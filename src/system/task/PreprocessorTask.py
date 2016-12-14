# MincePy
from system.task.AbstractTask import AbstractTask

class PreprocessorTask(AbstractTask):
    '''
    Performs required steps before any task modules are able to run.
    
    #. Calls initialize() on each :class:`.DatabaseConfiguration` to create
       working copies of original input databases.
    '''
    
    def execute(self):
        '''
        See :meth:`.AbstractTask.execute`.
        '''
        activeDBs = self.getSystem().getConfig().getActiveDBs()

        for db in activeDBs:
            log = self.getLog(db)
            log.info("Initializing database...")
            
            overwriteMode = self.getSystem().getConfig().isOverwrite()
            db.initialize(overwriteMode)
