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
 * @file thread_timer.cc
 * @brief 
 */

#include "thread_timer.h"
#include "paxos.h"

namespace alisql {

ThreadTimerService::ThreadTimerService()
  :loop_(NULL)
{
  ld_= new LoopData();
  loop_= ev_loop_new(0);
  ld_->lock.lock();
  ev_async_start(loop_, &ld_->asyncWatcher);
  ev_set_userdata(loop_, ld_);
  /* ev_set_invoke_pending_cb */
  ev_set_loop_release_cb(loop_, ThreadTimerService::loopRelease, ThreadTimerService::loopAcquire);
  ld_->lock.unlock();
  thread_= new std::thread(ThreadTimerService::mainService, loop_);
}

ThreadTimerService::~ThreadTimerService()
{
  ld_->lock.lock();
  ld_->shutdown= true;
  wakeup();
  ld_->lock.unlock();
  thread_->join();
  delete thread_;
  ld_->lock.lock();
  ev_loop_destroy(loop_);
  ld_->lock.unlock();
  loop_= NULL;
  thread_= NULL;
  delete ld_;
  ld_= NULL;
}

void ThreadTimerService::mainService(EV_P)
{
  LoopData *ld= (LoopData *) ev_userdata (EV_A);
  ld->lock.lock();
  ev_run (EV_A, 0);
  ld->lock.unlock();
}

void ThreadTimerService::loopRelease(EV_P)
{
  LoopData *ld= (LoopData *) ev_userdata(EV_A);
  ld->lock.unlock();
}

void ThreadTimerService::loopAcquire(EV_P)
{
  LoopData *ld= (LoopData *) ev_userdata(EV_A);
  ld->lock.lock();
}

void ThreadTimerService::loopAsync(EV_P, ev_async *w, int revents)
{
  LoopData *ld= (LoopData *) ev_userdata(EV_A);
  if (ld->shutdown)
    ev_break(EV_A, EVBREAK_ALL);
}


ThreadTimer::~ThreadTimer()
{
  stop();
}

void ThreadTimer::start()
{
  if (type_ == Stage)
    setStageExtraTime(time_);
  else if (type_ == Repeatable)
    currentStage_.store(0);

  service_->ldlock();
  ev_now_update(service_->getEvLoop());
  ev_timer_start(service_->getEvLoop(), &timer_);
  service_->wakeup();
  service_->ldunlock();
  auto loop= service_->getEvLoop();
  auto w= &timer_;
  easy_info_log("ThreadTimer::start ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
}

void ThreadTimer::restart(double t)
{
  assert(type_ == Repeatable || type_ == Stage);
  auto loop= service_->getEvLoop();
  auto w= &timer_;
  easy_info_log("ThreadTimer::restarts ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);

  if (t != 0.0)
    time_= t;
  if (type_ == Stage)
    setStageExtraTime(time_);
  else if (type_ == Repeatable)
    currentStage_.store(0);

  if (t == 0.0)
  {
    service_->ldlock();
    /* We should update the mn_now&ev_rt_now because the loop may suspend a long time. */
    ev_now_update(service_->getEvLoop());
    if (type_ == Stage)
      ev_timer_set(&timer_, time_, time_);
    ev_timer_again(service_->getEvLoop(), &timer_);
    service_->wakeup();
    service_->ldunlock();
  }
  else
  {
    service_->ldlock();
    /* We should update the mn_now&ev_rt_now because the loop may suspend a long time. */
    ev_now_update(service_->getEvLoop());
    ev_timer_set(&timer_, t, t);
    ev_timer_again(service_->getEvLoop(), &timer_);
    service_->wakeup();
    service_->ldunlock();
  }
  easy_info_log("ThreadTimer::restarte ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
}

void ThreadTimer::restart()
{
  restart(0.0);
}

void ThreadTimer::restart(uint64_t t, bool needRandom)
{
  double f= (double)t;
  f /= 1000;

  restart(f);
}

void ThreadTimer::stop()
{
  if (ev_is_active(&timer_))
  {
    service_->ldlock();
    ev_timer_stop(service_->getEvLoop(), &timer_);
    service_->wakeup();
    service_->ldunlock();

    auto loop= service_->getEvLoop();
    auto w= &timer_;
    easy_info_log("ThreadTimer::stop ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
  }
}

void ThreadTimer::setStageExtraTime(double baseTime)
{
  assert(type_ == Stage);
  uint64_t base= 10000, randn= rand() % base;
  if (randWeight_ != 0)
  {
    /* if weigth == 9 we use lest time here. */
    randn= randn/10 + 1000 * (9 - randWeight_);
  }
  double tmp= baseTime * randn / base;
  stageExtraTime_ = (tmp < 0.001 ? 0.001 : tmp);
  currentStage_.store(0);
}

void ThreadTimer::setDelayFlag(bool flag)
{
	easy_warn_log("ThreadTimer set delayFlag: %d", flag);
  if (delayFlag_.load() == flag)
    return;
  delayFlag_.store(flag);
}

void ThreadTimer::setDisableDelay(bool flag)
{
  assert(type_ == Stage || type_ == Repeatable);
	easy_info_log("ThreadTimer set disableDelayFlag: %d", flag);
  if (disableDelayFlag_.load() == flag)
    return;
  disableDelayFlag_.store(flag);
}

void ThreadTimer::resetCurrentDelays()
{
  assert(type_ == Stage || type_ == Repeatable);
	easy_warn_log("ThreadTimer reset delayCounts");
  currentDelayCounts_.store(0);
}

void ThreadTimer::setDelayTimeout(uint64_t delayTimeout)
{
  double ft= ((double)delayTimeout)/1000;
  uint64_t delayCounts = (uint64_t) (ft / time_); 

	easy_warn_log("ThreadTimer set delayTimeout: %ld", delayTimeout);

  assert(type_ == Stage || type_ == Repeatable);

  if (delayCounts < 1)
    delayCounts = 1;

  totalDelayCounts_.store(delayCounts);
}

uint64_t ThreadTimer::getAndSetStage()
{
  bool delayEnabled = delayFlag_.load() && !disableDelayFlag_.load();
  uint64_t newStage = 0;
  
  easy_debug_log("ThreadTimer stage from %ld, delayFlag: %d, disableDelayFlag: %d", 
  		currentStage_.load(), delayFlag_.load(), disableDelayFlag_.load());

  if (currentStage_.load() == 0)
  {
    currentStage_.store(delayEnabled ? 1 : 2);
    currentDelayCounts_.store(0);
    newStage = delayEnabled ? 1 : 2;
  }
  else if (currentStage_.load() == 1)
  {
    easy_warn_log("ThreadTimer in delay stage 1, currentDelays: %ld/%ld", 
                      currentDelayCounts_.load(), totalDelayCounts_.load());
    if (delayEnabled && currentDelayCounts_.fetch_add(1) < totalDelayCounts_.load())
    {
      newStage = 1;
    }
    else
    {
      currentStage_.store(2);
      newStage = 2;
    }
  }
  else 
  {
    currentStage_.store(0);
    newStage = 0;
  }

  easy_debug_log("ThreadTimer stage to %ld, delayFlag: %d, disableDelayFlag: %d", 
          newStage, delayFlag_.load(), disableDelayFlag_.load());

  return newStage;
}

void ThreadTimer::callbackRunWeak(CallbackWeakType callBackPtr)
{
  if (auto spt = callBackPtr.lock())
    spt->run();
  else
    easy_error_log("ThreadTimer::callbackRun : the callBackPtr already be deteled, stop this async call.");
}

void ThreadTimer::callbackRun(CallbackType callBackPtr)
{
  if (callBackPtr != nullptr)
    callBackPtr->run();
}

void ThreadTimer::timerCallbackInternal(struct ev_loop *loop, ev_timer *w, int revents)
{
  easy_info_log("timerCallbackInternal ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
  ThreadTimer *tt= (ThreadTimer *)(w->data);
  if (tt->getType() == ThreadTimer::Stage)
  {
    double t;
    uint64_t next_stage = tt->getAndSetStage();
    if (next_stage == 2)
    {
      t= tt->getStageExtraTime();
      easy_debug_log("timerCallbackInternal ThreadTimer change stage to 2, extraTime:%lf", t);
    }
    else if (next_stage == 1)
    {
      t= tt->getTime();
      easy_warn_log("timerCallbackInternal ThreadTimer in delay stage 1, delayTime:%lf", t);
    }
    else
    {
      t= tt->getTime();
      if (tt->getService() != nullptr)
      {
        assert(tt->callBackPtr != nullptr);
        tt->getService()->sendAsyncEvent(ThreadTimer::callbackRunWeak, std::move(CallbackWeakType(tt->callBackPtr)));
      }
      else
        tt->callBackPtr->run();
    }
    ev_timer_set(w, t, t);
    ev_timer_again(loop, w);
    easy_info_log("timerCallbackInternal Stage ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
  }
  else
  {
    uint64_t next_stage = tt->getAndSetStage();
    if (next_stage == 1)
    {
      easy_warn_log("ThreadTimer Repeatable in delay stage 1, delayTime:%lf", tt->getTime());
      ev_timer_again(loop, w);
      easy_info_log("timerCallbackInternal Repeatable ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
    }
    else if (tt->getService() != nullptr) //&& tt->getType() != ThreadTimer::Oneshot)
    {
      /* Oneshot should also use asyc call.
         otherwise deadlock will happened between LoopData::lock and Paxos::lock. */
      if (tt->getType() != ThreadTimer::Oneshot)
        tt->getService()->sendAsyncEvent(ThreadTimer::callbackRunWeak, std::move(CallbackWeakType(tt->callBackPtr)));
      else
        tt->getService()->sendAsyncEvent(ThreadTimer::callbackRun, tt->callBackPtr);
    }
    else
      tt->callBackPtr->run();
    if (tt->getType() == ThreadTimer::Oneshot)
      delete tt;
  }
}
};/* end of namespace alisql */
