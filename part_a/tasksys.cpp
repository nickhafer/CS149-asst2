#include "tasksys.h"
#include <thread>
#include <atomic>
#include <iostream>
#include <vector>
#include <mutex>
#include <condition_variable>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    // set up threads
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // serial code:
    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // parallel code:
    std::thread *threads = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        threads[i] = std::thread([runnable, num_total_tasks, i, this]() {
            for (int j = i; j < num_total_tasks; j += num_threads) {
                runnable->runTask(j, num_total_tasks);
            } });
    }

    // wait for threads to finish
    for (int i = 0; i < num_threads; i++)
    {
        threads[i].join();
    }

    // clean up
    delete[] threads;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //


    this->overall_ctr = 0;
    this->num_threads = num_threads;
    this->threads = std::vector<std::thread>(num_threads);
    this->next_task = 0;
    this->num_tasks = 0;
    this->tasks_completed = 0;
    this->member_runnable = nullptr;
    this->dead = false;

    // create threads that will spin and check for tasks
    for (int i = 0; i < num_threads; i++)
    {
        threads[i] = std::thread([this]()
                                 {
            // while (true) {
            //     if (dead) break;

            //     int task_index;
            //     {
            //         std::lock_guard<std::mutex> lock(m);
            //         task_index = next_task.fetch_add(1);
            //     }

            //     if (task_index >= num_tasks) {
            //         continue;
            //     }

            //     member_runnable->runTask(task_index, num_tasks);

            //     {
            //         std::lock_guard<std::mutex> lock(m);
            //         tasks_completed.fetch_add(1);
            //     }
            // } });
            while (true) {
                if (dead) break;

                // Batch task processing
                int start_task;
                {
                    std::lock_guard<std::mutex> lock(m);
                    start_task = next_task.fetch_add(16); //fetch 16 tasks per thread at once
                }
                if (start_task >= num_tasks) {
                    continue;
                }

                int end_task = std::min(start_task + 16, num_tasks);
                for (int task_index = start_task; task_index < end_task; ++task_index) {
                    member_runnable->runTask(task_index, num_tasks);
                }

                {
                    std::lock_guard<std::mutex> lock(m);
                    tasks_completed.fetch_add(end_task - start_task);
                }
            } });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::lock_guard<std::mutex> lock(m);
        dead = true;
    }
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    overall_ctr++;

    // printf("NEW RUN STARTS WITH ** %d ** TOTAL TASKS and run # %d \n", num_tasks, overall_ctr);

    {
        std::lock_guard<std::mutex> lock(m);
        tasks_completed = 0;
        next_task = 0;
        num_tasks = num_total_tasks;
        member_runnable = runnable;
    }

    while (true) {
        {
            std::lock_guard<std::mutex> lock(m);
            if (tasks_completed.load() == num_total_tasks) {
                break;
            }
        }
    }

    member_runnable = nullptr;

    // printf("RUN ENDS WITH ** %d ** TOTAL TASKS and run # %d\n", num_tasks, overall_ctr);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    this->num_threads = num_threads;
    this->threads = std::vector<std::thread>(num_threads);
    this->next_task = 0;
    this->num_tasks = 0;
    this->tasks_completed = 0;
    this->member_runnable = nullptr;
    this->dead = false;

    // Create worker threads
    for (int i = 0; i < num_threads; i++)
    {
        threads[i] = std::thread([this]()
                                 {
        while (true) {
            std::unique_lock<std::mutex> lock(m);

            // Sleep until there's work to do or thread pool is shutting down
            cv_worker.wait(lock, [this]() { 
                return dead || (member_runnable != nullptr && next_task < num_tasks); 
            });

            if (dead) break;

            // Grab multiple tasks at once to reduce lock contention
            int start_task = next_task;
            int tasks_to_take = std::min(32, num_tasks - start_task); // Take up to 32 tasks
            next_task += tasks_to_take;

            lock.unlock();
            
            // Process multiple tasks without needing the lock
            for (int i = 0; i < tasks_to_take; i++) {
                member_runnable->runTask(start_task + i, num_tasks);
            }
                    
            lock.lock();
            tasks_completed += tasks_to_take;

            if (tasks_completed == num_tasks) {
                cv_main.notify_one();
            }
        } });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::lock_guard<std::mutex> lock(m);
        dead = true;
    }
    cv_worker.notify_all(); // Wake up all threads for shutdown
    for (int i = 0; i < num_threads; i++)
    {
        threads[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    
    std::unique_lock<std::mutex> lock(m);
    tasks_completed = 0;
    next_task = 0;
    num_tasks = num_total_tasks;
    member_runnable = runnable;

    // Wake up worker threads
    cv_worker.notify_all();

    // Wait for all tasks to complete
    cv_main.wait(lock, [this]()
                    { return tasks_completed == num_tasks; });

    member_runnable = nullptr;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
