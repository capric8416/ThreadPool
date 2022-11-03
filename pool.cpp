// self
#include "pool.h"

// project
#include "../logger/logger.h"



TaskPool::TaskPool(int32_t maxThreads)
    : m_pThreadWorker(nullptr)
    , m_interval(0)
    , m_maxThreads(maxThreads)
{
    // 限制最大线程数为处理器逻辑线程数的4倍
    uint32_t logicThreads = 4 * std::thread::hardware_concurrency();
    if (maxThreads <= 0 || maxThreads > logicThreads) {
        maxThreads = logicThreads;
    }
    m_maxThreads = maxThreads;

    Start();
}


TaskPool::~TaskPool()
{
    Stop();
}


TaskPool *TaskPool::GetInstance(int32_t interval, int32_t maxThreads)
{
    static TaskPool instance(maxThreads);
    instance.SetInterval(interval);

    return &instance;
}


void TaskPool::ThreadTimerLoop(void *ptr)
{
    TaskPool *pThis = (TaskPool *)ptr;

    LogInfoC("started\n");

    while (pThis->m_running) {
        auto begin = std::chrono::high_resolution_clock::now();

        if (!pThis->m_IntevalTasks.empty()) {
            if (!pThis->m_running) {
                break;
            }

            std::lock_guard<std::mutex> locker1(pThis->m_mutexIntervalTasks);

            if (!pThis->m_running) {
                break;
            }

            // 执行周期性任务
            for (auto iter = pThis->m_IntevalTasks.begin(); iter != pThis->m_IntevalTasks.end(); iter++) {
                if (!pThis->m_running) {
                    break;
                }

                iter->first -= pThis->m_interval;
                if (iter->first > 0) {
                    continue;
                }

                TaskInfo intervalInfo = iter->second;
                iter->first = intervalInfo.ms;  // 重置倒计时

                // 送到线程池去执行
                pThis->SubmitOnceTask(intervalInfo);
            }
        }

        if (!pThis->m_running) {
            break;
        }

        // 休眠
        auto left = std::chrono::milliseconds(pThis->m_interval) - std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin);
        if (left.count() > 0) {
            std::this_thread::sleep_for(left);
        }
    }

    LogInfoC("stopped\n");
}


void TaskPool::ThreadTaskLoop(void *ptr)
{
    TaskPool *pThis = (TaskPool *)ptr;

    LogInfoC("started\n");

    while (pThis->m_running) {
        std::unique_lock<std::mutex> locker1(pThis->m_mutexPendingTasksWait);

        if (std::cv_status::no_timeout == pThis->m_cvPendingTasks.wait_for(locker1, std::chrono::milliseconds(100))) {
            // lock
            if (!pThis->m_mutexPendingTasksOpt.try_lock_for(std::chrono::milliseconds(3))) {
                continue;
            }

            if (pThis->m_PendingTasks.empty()) {
                // unlock
                pThis->m_mutexPendingTasksOpt.unlock();
                continue;
            }

            // pop task
            auto taskInfo = pThis->m_PendingTasks.front();
            pThis->m_PendingTasks.pop();

            // unlock
            pThis->m_mutexPendingTasksOpt.unlock();

            if (taskInfo.name.empty()) {
                continue;
            }

            // execute task
            if (taskInfo.func(taskInfo.ptr)) {
                taskInfo.success(taskInfo.ptr);
            }
            else {
                taskInfo.fail(taskInfo.ptr);
                LogWarningC("execute task fail, name: %s, ptr: %p\n", taskInfo.name.c_str(), taskInfo.ptr);
            }
        }
    }

    LogInfoC("stopped\n");
}


void TaskPool::Start()
{
    LogInfoC("started\n");

    m_running = true;
    m_pThreadWorker = new std::thread(ThreadTimerLoop, this);

    for (int i = 0; i < m_maxThreads; i++) {
        std::thread *t = new std::thread(ThreadTaskLoop, this);
        m_backgroundThreads.push_back(t);
    }
}


void TaskPool::Stop(bool join)
{
    m_running = false;

    // 清理定时任务列表，等待线程退出
    {
        std::lock_guard<std::mutex> locker(m_mutexIntervalTasks);

        m_IntevalTasks.clear();

        if (join && m_pThreadWorker != nullptr) {
            m_pThreadWorker->join();

            delete m_pThreadWorker;
            m_pThreadWorker = nullptr;
        }
    }

    // 清理线程池
    while (true) {
        if (!m_mutexPendingTasksOpt.try_lock_for(std::chrono::milliseconds(16))) {
            continue;
        }

        if (join) {
            while (!m_backgroundThreads.empty()) {
                m_cvPendingTasks.notify_all();

                std::thread *t = m_backgroundThreads.front();
                t->join();

                delete t;
                t = nullptr;

                m_backgroundThreads.pop_front();
            }
        }

        m_mutexPendingTasksOpt.unlock();

        break;
    }

    if (join) {
        LogInfoC("stopped\n");
    }
}


void TaskPool::SetInterval(int32_t value)
{
    if (value != m_interval && value > 0 && value <= 1000) {
        m_interval = value;
    }
}


void TaskPool::SubmitOnceTask(std::string name, void *ptr, TaskFunc func, TaskFunc OnSuccess, TaskFunc OnFail)
{
    TaskInfo info;
    info.ms = 0;
    info.ptr = ptr;
    info.name = name;
    info.func = func;
    info.success = OnSuccess;
    info.fail = OnFail;

    SubmitOnceTask(info);
}


void TaskPool::SubmitOnceTask(TaskInfo &info)
{
    while (true) {
        if (!m_mutexPendingTasksOpt.try_lock_for(std::chrono::milliseconds(16))) {
            continue;
        }

        SubmitOnceTaskWithoutLock(info);

        m_mutexPendingTasksOpt.unlock();

        break;
    }
}


void TaskPool::SubmitOnceTaskWithoutLock(std::string name, void *ptr, TaskFunc func, TaskFunc OnSuccess, TaskFunc OnFail)
{
    TaskInfo info;
    info.ms = 0;
    info.ptr = ptr;
    info.name = name;
    info.func = func;
    info.success = OnSuccess;
    info.fail = OnFail;

    // 送到线程池去执行
    m_PendingTasks.push(info);
    m_cvPendingTasks.notify_one();
}


void TaskPool::SubmitOnceTaskWithoutLock(TaskInfo &info)
{
    // 送到线程池去执行
    m_PendingTasks.push(info);
    m_cvPendingTasks.notify_one();
}


void TaskPool::SubmitIntervalTask(std::string name, void *ptr, int32_t ms, TaskFunc func, TaskFunc OnSuccess, TaskFunc OnFail)
{
    std::lock_guard<std::mutex> locker(m_mutexIntervalTasks);

    TaskInfo info;
    info.ms = ms;
    info.ptr = ptr;
    info.name = name;
    info.func = func;
    info.success = OnSuccess;
    info.fail = OnFail;

    m_IntevalTasks.push_back(std::make_pair(ms, info));

    // 按执行周期升序排列
    std::sort(
        m_IntevalTasks.begin(), m_IntevalTasks.end(),
        [](std::pair<int32_t, TaskInfo> v1, std::pair<int32_t, TaskInfo> v2) {
            return v1.first < v2.first;
        }
    );
}


void TaskPool::UpdateIntervalTask(std::string name, int32_t ms)
{
    std::lock_guard<std::mutex> locker(m_mutexIntervalTasks);

    for (auto iter = m_IntevalTasks.begin(); iter != m_IntevalTasks.end(); iter++) {
        if (iter->second.name == name) {
            iter->second.ms = ms;
            break;
        }
    }
}


void TaskPool::RemoveIntervalTask(std::string name, bool lock)
{
    if (lock) {
        std::lock_guard<std::mutex> locker(m_mutexIntervalTasks);

        for (auto iter = m_IntevalTasks.begin(); iter != m_IntevalTasks.end(); iter++) {
            if (iter->second.name == name) {
                m_IntevalTasks.erase(iter);
                break;
            }
        }
    }
    else {
        // 避免在定时任务里面去移除定时任务时发生的的锁重入的问题
        SubmitOnceTaskWithoutLock(
            "Remove" + name, this,
            [name](void *ptr) {
                TaskPool *pThis = (TaskPool *)ptr;
                pThis->RemoveIntervalTask(name);
                return true;
            }
        );
    }
}
