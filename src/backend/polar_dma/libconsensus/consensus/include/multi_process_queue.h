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
 * @file multi_process_queue.h
 * @brief 
 */

#ifndef  multi_process_queue_INC
#define  multi_process_queue_INC

//#include "single_process_queue.h"

namespace alisql {

template<typename TaskType>
//class MultiProcessQueue : public SingleProcessQueue<TaskType> {
class MultiProcessQueue {
  public:
    explicit MultiProcessQueue(uint64_t maxConc) :stop_(false), maxConc_(maxConc), curConc_(0) {onProcess_.store(false);}
    virtual ~MultiProcessQueue () {};
    bool push(TaskType *task) {
      std::lock_guard<std::mutex> lg(lock_);
      if (!stop_)
      {
        taskList_.push(task);
        return (curConc_.load() < maxConc_.load());
      }
      else
        return false;
    }
    void stop() {
      lock_.lock();
      stop_= true;
      while (!taskList_.empty())
      {
        TaskType *task= taskList_.front();
        taskList_.pop();
        delete task;
      }
      lock_.unlock();
      while (curConc_.load() > 0) ;
    }
    void multiProcess(std::function<void(TaskType*)> cb) {
      uint64_t tasks;
      lock_.lock();
      if (curConc_.load() >= maxConc_.load() || stop_)
      {
        lock_.unlock();
        return;
      }
      else
      {
        curConc_.fetch_add(1);
        lock_.unlock();
      }

      for (;;)
      {
        lock_.lock();
        tasks= taskList_.size();
        if (tasks == 0)
        {
          curConc_.fetch_sub(1);
          lock_.unlock();
          break;
        }
        TaskType *task= taskList_.front();
        taskList_.pop();
        lock_.unlock();

        cb(task);
        delete task;
      }
    }

  protected:
    std::queue<TaskType *> taskList_;
    std::mutex lock_;
    std::atomic<bool> onProcess_;
    bool stop_;

    std::atomic<uint64_t> maxConc_;
    std::atomic<uint64_t> curConc_;

  private:
    MultiProcessQueue ( const MultiProcessQueue &other );   // copy constructor
    const MultiProcessQueue& operator = ( const MultiProcessQueue &other ); // assignment operator
};


}
#endif     //#ifndef multi_process_queue_INC 
