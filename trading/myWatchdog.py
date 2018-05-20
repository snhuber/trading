from ib_insync.ibcontroller import Watchdog
from ib_insync import IB
from ib_insync import util
import apscheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

class myWatchdog(Watchdog):
    """
    class that overrides the default stop and scheduleRestart behaviour.
    The class has a member that is the list of the schedulers and schedules and kills all jobs when there is a stop.
    When there is a restart, it restarts the jobs.
    """
    def __init__(self,*args,**kwargs):
        self. schedulerList = [[None,None]] # list of list of [scheduler, schedules]
        util.patchAsyncio()
        super(myWatchdog,self).__init__(*args, **kwargs)


    def stop(self):
        self._logger.info('Stopping mywatchdog')
        try:
            loop = asyncio.get_event_loop()
            # for i,t in enumerate(asyncio.Task.all_tasks(loop)):
            #     self._logger.info(f'task: {i}')
            #     t.cancel()
            # self._logger.info('Now stopping loop')
            # loop.close()
            # self._logger.info('loop stopped')
            # self.s1.remove()
        except:
            pass

        for i, (scheduler, schedules) in enumerate(self.schedulerList):
            # when the network is down, the system tries to reconnect and does not succeed.
            # but the list of jobs is already empty (because it was emptied during the shutdown process)
            # therefore, we need to get new jobdict information only if there are jobs in the scheduler
            if isinstance(scheduler, AsyncIOScheduler) and scheduler.running:
                if len(scheduler.get_jobs()) > 0:
                    schedules = []
                    for job in scheduler.get_jobs():
                        if isinstance(job.trigger, apscheduler.triggers.cron.CronTrigger):
                            jobdict = {
                                'func': job.func,
                                'args': job.args,
                                'kwargs': job.kwargs,
                                'id': job.id,
                                'misfire_grace_time': job.misfire_grace_time,
                                'coalesce': job.coalesce,
                                'max_instances': job.max_instances,
                                'next_run_time': job.next_run_time,
                                'jobstore': job._jobstore_alias,
                                'trigger': 'cron'}
                            for f in job.trigger.fields:
                                curval = str(f)
                                jobdict[f.name] = curval
                                pass
                            pass
                            schedules.append(jobdict)
                            pass
                        job.remove()
                        pass
                    pass
                pass
            self.schedulerList[i][1] = schedules

            self._logger.info(f'scheduler: {scheduler} {type(scheduler)}')
            if isinstance(scheduler,AsyncIOScheduler) and scheduler.running:
                self._logger.info(f'scheduler: {scheduler}, {scheduler.running}')
                IB.sleep(2)
                scheduler.shutdown(wait=False)
                pass

        self._logger.info('Stopped mywatchdog')
        self._logger.info('Start Scheduler List for mywatchdog')
        for scheduler, schedules in self.schedulerList:
            self._logger.info(f'mywatchdog: scheduler: {scheduler}, {type(scheduler)}')
            if isinstance(scheduler,AsyncIOScheduler) and scheduler.running:
                for schedule in schedules:
                    self._logger.info(f'mywatchdog: schedule: {schedule}')
                    pass
                pass
            pass
        self._logger.info('End Scheduler List for mywatchdog')

        self.ib.disconnect()
        self.controller.terminate()


    def start(self):
        self._logger.info('Starting mywatchdog')
        self.controller.start()
        IB.sleep(self.appStartupTime)
        try:
            self.ib.connect(self.host, self.port, self.clientId,
                    self.connectTimeout)
            self.ib.setTimeout(self.appTimeout)
        except:
            # a connection failure will be handled by the apiError callback
            pass

        self._logger.info('Restarted mywatchdog')
        self._logger.info('Start Scheduler List for mywatchdog')
        for scheduler, schedules in self.schedulerList:
            self._logger.info(f'mywatchdog: scheduler: {scheduler}, {type(scheduler)}')
            if isinstance(schedules,list) and isinstance(scheduler,AsyncIOScheduler) and scheduler.running:
                for schedule in schedules:
                    scheduler.add_job(**schedule)
                    self._logger.info(f'mywatchdog: schedule: {schedule}')
                    pass
                scheduler.start()
                pass
            pass
        self._logger.info('End Scheduler List for mywatchdog')