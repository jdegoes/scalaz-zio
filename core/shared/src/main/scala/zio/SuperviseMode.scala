/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio

import zio.internal.FiberContext

/**
 * Dictates the supervision mode when a child fiber is forked from a parent
 * fiber. There are three possible supervision modes: `Disown`, `Await`,
 * and `Interrupt`, which determine what the parent fiber will do with the
 * child fiber when the parent fiber exits.
 */
final case class SuperviseMode(run: (Exit[Any, Any], Fiber[Any, Any]) => UIO[Any])

object SuperviseMode {

  /**
   * The child fiber will be disowned when the parent fiber exits.
   */
  val disown: SuperviseMode = SuperviseMode {
    case (_, fiber: FiberContext[_, _]) => UIO.effectTotal(Fiber.track(fiber))
    case _ => UIO.unit
  }

  /**
   * The child fiber will be awaited when the parent fiber exits.
   */
  val await: SuperviseMode = SuperviseMode((_, fiber) => fiber.await)

  /**
    * The child fiber will be awaited when the parent fiber exits normally and
    * interrupted otherwise.
    */
  val inherit: SuperviseMode = SuperviseMode {
    case (Exit.Success(_), fiber) => fiber.await
    case (Exit.Failure(_), fiber) => fiber.interrupt
  }

  /**
   * The child fiber will be interrupted when the parent fiber exits.
   */
  val interrupt: SuperviseMode = SuperviseMode((_, fiber) => fiber.interrupt)

  /**
   * The child fiber will be interrupted when the parent fiber exits, but in
   * the background, without blocking the parent fiber.
   */
  val interruptFork: SuperviseMode = SuperviseMode((_, fiber) => fiber.interrupt.forkDaemon)
}
