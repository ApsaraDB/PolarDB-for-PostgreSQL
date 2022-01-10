/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @file single_process_queue.h
 * @brief 
 */

#ifndef  single_process_queue_INC
#define  single_process_queue_INC

#include <atomic>
#include <queue>
#include <mutex>


namespace alisql {

/**
 * @class SingleProcessQueue
 *
 * @brief 
 *
 **/
template<typename TaskType>
class SingleProcessQueue {
  public:
    SingleProcessQueue () :stop_(false) {onProcess_.store(false);}
    virtual ~SingleProcessQueue () {};
    bool push(TaskType *task) {
      std::lock_guard<std::mutex> lg(lock_);
      if (!stop_)
      {
        taskList_.push(task);
        return !onProcess_.load();
      }
      else
        return false;
    }
    void start() {
      std::lock_guard<std::mutex> lg(lock_);
      stop_= false;
    }
    /* true: the caller should delete queue. */
    bool stop(bool wait= false) {
      bool ret= true;
      lock_.lock();
      stop_= true;
      while (!taskList_.empty())
      {
        TaskType *task= taskList_.front();
        taskList_.pop();
        delete task;
      }
      if (!wait && onProcess_.load())
      {
        taskList_.push(NULL);
        ret= false;
      }
      lock_.unlock();
      if (wait)
        while (onProcess_.load()) ;
      return ret;
    }
    void process(std::function<void(TaskType*)> cb) {
      uint64_t tasks;
      lock_.lock();
      if (onProcess_.load() || stop_)
      {
        lock_.unlock();
        return;
      }
      else
      {
        onProcess_.store(true);
        lock_.unlock();
      }

      for (;;)
      {
        lock_.lock();
        tasks= taskList_.size();
        if (tasks == 0)
        {
          onProcess_.store(false);
          lock_.unlock();
          break;
        }
        TaskType *task= taskList_.front();
        taskList_.pop();
        lock_.unlock();

        if (task == NULL)
          return;
        cb(task);
        delete task;
      }
    }
    void mergeableProcess(std::function<void(TaskType*)> cb) {
      uint64_t tasks;
      lock_.lock();
      if (onProcess_.load() || stop_)
      {
        lock_.unlock();
        return;
      }
      else
      {
        onProcess_.store(true);
        lock_.unlock();
      }

      for (;;)
      {
        uint64_t mergedTaskNum= 1;
        lock_.lock();
        tasks= taskList_.size();
        if (tasks == 0)
        {
          onProcess_.store(false);
          lock_.unlock();
          break;
        }
        TaskType *task= taskList_.front();
        taskList_.pop();
        while (taskList_.size() >= 1)
        {
          TaskType *nextTask= taskList_.front();
          if (nextTask == NULL)
          {
            delete task;
            task= NULL;
            break;
          }
          if (task->merge(nextTask))
          {
            ++ mergedTaskNum;
            taskList_.pop();
            delete nextTask;
          }
          else
            break;
        }
        lock_.unlock();
        if (task == NULL)
          return;

        if (mergedTaskNum >= 2)
          task->printMergeInfo(mergedTaskNum);
        cb(task);
        delete task;
      }
    }

    static void mergeableProcessWeak(std::weak_ptr<SingleProcessQueue<TaskType>> wqueue, std::function<void(TaskType*)> cb)
    {
      // skip the async call if object has been deleted
      if (auto q = wqueue.lock())
        q->mergeableProcess(cb);
    }

    void mergeableSameProcess(std::function<void(TaskType*)> cb) {
      uint64_t tasks;
      lock_.lock();
      if (onProcess_.load() || stop_)
      {
        lock_.unlock();
        return;
      }
      else
      {
        onProcess_.store(true);
        lock_.unlock();
      }

      for (;;)
      {
        uint64_t mergedTaskNum= 1;
        lock_.lock();
        tasks= taskList_.size();
        if (tasks == 0)
        {
          onProcess_.store(false);
          lock_.unlock();
          break;
        }
        TaskType *task= taskList_.front();
        taskList_.pop();
        while (taskList_.size() >= 1)
        {
          TaskType *nextTask= taskList_.front();
          if (nextTask == NULL)
          {
            delete task;
            task= NULL;
            break;
          }
          ++ mergedTaskNum;
          taskList_.pop();
          delete nextTask;
        }
        lock_.unlock();
        if (task == NULL)
          return;

        cb(task);
        if (mergedTaskNum > 100)
          msleep_(2);
        delete task;
      }
    }

  protected:
    void msleep_(uint64_t t)
    {
      struct timeval sleeptime;
      if (t == 0)
        return;
      sleeptime.tv_sec= t / 1000;
      sleeptime.tv_usec= (t - (sleeptime.tv_sec * 1000)) * 1000;
      select(0, 0, 0, 0, &sleeptime);
    }

  protected:
    std::queue<TaskType *> taskList_;
    std::mutex lock_;
    std::atomic<bool> onProcess_;
    bool stop_;

  private:
    SingleProcessQueue ( const SingleProcessQueue &other );   // copy constructor
    const SingleProcessQueue& operator = ( const SingleProcessQueue &other ); // assignment operator

};/* end of class SingleProcessQueue */
} //namespace alisql

#endif     //#ifndef single_process_queue_INC 
