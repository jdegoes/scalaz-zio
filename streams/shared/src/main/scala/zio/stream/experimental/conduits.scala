package zio.stream.experimental

import zio._

sealed trait ZConduit[-In, -R, +E, +Out, +Z] { self =>
  def &&&[In2, R1 <: R, E1 >: E, Out2, Z2](
    that: => ZConduit[In2, R1, E1, Out2, Z2]
  ): ZConduit[(In, In2), R1, E1, Either[Out, Out2], (Z, Z2)] = ZConduit.Parallel(self, that)

  def ++[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out](
    that: => ZConduit[In1, R1, E1, Out1, Any]
  ): ZConduit[In1, R1, E1, Out1, Any] =
    ZConduit.concatAllWith(ZConduit.emitAll(Chunk(self, that)))((): Any)((_, _) => ())

  def <++>[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, (Z, Z2)] = {
    type C = ZConduit[In1, R1, E1, Out1, List[Either[Z, Z2]]]

    ZConduit
      .concatAllWith(
        ZConduit.emitAll[C](Chunk(self.map(v => List(Left(v))), that.map(v => List(Right(v))))).map(_ => Nil)
      )(List.empty[Either[Z, Z2]])(_ ++ _)
      .map { list =>
        val left  = list.collectFirst { case Left(l) => l }.get
        val right = list.collectFirst { case Right(r) => r }.get

        (left, right)
      }
  }

  def ++>[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z2] =
    (self <++> that).map(_._2)

  def <++[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z] =
    (self <++> that).map(_._1)

  def >>>[R1 <: R, E1 >: E, Out2](that: => ZConduit[Out, R1, E1, Out2, Any]): ZConduit[In, R1, E1, Out2, Any] =
    self pipeToRight that

  def *>[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z2] = (self zip that).map(_._2)

  def <*[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z] = (self zip that).map(_._1)

  def pipeTo[R1 <: R, E1 >: E, Out2](that: => ZConduit[Out, R1, E1, Out2, Any]): ZConduit[In, R1, E1, Out2, Any] =
    self pipeToRight that

  def pipeToBoth[R1 <: R, E1 >: E, Out2, Z2](
    that: => ZConduit[Out, R1, E1, Out2, Z2]
  ): ZConduit[In, R1, E1, Out2, (Z, Z2)] =
    ZConduit.Pipe(() => self, () => that)

  def pipeToLeft[R1 <: R, E1 >: E, Out2](that: => ZConduit[Out, R1, E1, Out2, Any]): ZConduit[In, R1, E1, Out2, Z] =
    (self pipeToBoth that).map(_._1)

  def pipeToRight[R1 <: R, E1 >: E, Out2, Z2](
    that: => ZConduit[Out, R1, E1, Out2, Z2]
  ): ZConduit[In, R1, E1, Out2, Z2] =
    (self pipeToBoth that).map(_._2)

  def as[Z2](z2: => Z2): ZConduit[In, R, E, Out, Z2] = self.map(_ => z2)

  def catchAll[In1 <: In, R1 <: R, E2, Out1 >: Out, Done2](
    f: E => ZConduit[In1, R1, E2, Out1, Done2]
  ): ZConduit[In1, R1, E2, Out1, Done2] =
    ZConduit.CatchAll(self, f)

  def concatMap[In1 <: In, R1 <: R, E1 >: E, Out2](
    f: Out => ZConduit[In1, R1, E1, Out2, Any]
  ): ZConduit[In1, R1, E1, Out2, Any] =
    ZConduit.concatAll(self.mapOut(f))

  def concatMapWith[In1 <: In, R1 <: R, E1 >: E, Out2, Z1 >: Z](
    f: Out => ZConduit[In1, R1, E1, Out2, Z1]
  )(z1: Z1)(g: (Z1, Z1) => Z1): ZConduit[In1, R1, E1, Out2, Z1] =
    ZConduit.concatAllWith(self.mapOut(f))(z1)(g)

  def collect[Out2](f: PartialFunction[Out, Out2]): ZConduit[In, R, E, Out2, Z] =
    self pipeToLeft
      ZConduit.readChunk[Out].flatMap(chunk => ZConduit.emitAll(chunk.collect(f))).repeated.catchAll(_ => ZConduit.unit)

  def concatOut[In1 <: In, R1 <: R, E1 >: E, Out2](implicit
    ev: Out <:< ZConduit[In1, R1, E1, Out2, Any]
  ): ZConduit[In1, R1, E1, Out2, Any] =
    ZConduit.concatAll(self.mapOut(ev))

  def doneCollect: ZConduit[In, R, E, Nothing, (Chunk[Out], Z)] = ZConduit.DoneCollect(self)

  def emitCollect: ZConduit[In, R, E, (Chunk[Out], Z), Unit] = ZConduit.EmitCollect(self)

  def flatMap[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    f: Z => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z2] =
    ZConduit.FlatMap(self, f)

  def flatten[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](implicit
    ev: Z <:< ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z2] =
    self.flatMap(ev)

  def launch[R1 <: R, E1 >: E, In1 <: In](
    input: ZIO[R1, E1, Chunk[In1]],
    output: Chunk[Out] => ZIO[R1, E1, Any]
  ): ZIO[R1, Nothing, (UIO[Either[E1, Z]], IO[E1, Z])] =
    ZScope.make[Any].zip(DelayedRef.make[() => Either[E1, Z]]).flatMap { case (scope, getterRef) =>
      ZIO
        .access[R1] { env =>
          import ZConduit._

          val _ = (input, output)

          def loop[In, R, E, Out, Z](
            scope: ZScope[Any],
            getterRef: DelayedRef[() => Either[E, Z]],
            env: R,
            in: ZIO[R, E, Chunk[In]],
            leftover: Chunk[In],
            conduit: ZConduit[In, R, E, Out, Z]
          ): IO[E, Z] = {
            val _ = leftover

            conduit match {
              case Pipe(_, _) => ???
              case read: Read[In, R, E, Out, Z] =>
                getterRef.set(read.done) *>
                  (if (leftover.nonEmpty) loop(scope, getterRef, env, in, Chunk.empty, read.more(leftover))
                   else in.flatMap(in0 => loop(scope, getterRef, env, in, Chunk.empty, read.more(in0))).provide(env))
              case Done(terminal) => UIO(terminal(env))
              case Halt(cause)    => ZIO.halt(cause())
              case Effect(zio)    => zio.provide(env)
              case EmitAll(_)     => ???
              case MapOut(_, _)   => ???
              case Ensuring(next, finalizer) =>
                scope.ensure(_ => finalizer.provide(env)) *> // todo: track keys for expedited release
                  loop(scope, getterRef, env, in, leftover, next)
              case ConcatAll(_, _, _) => ???
              case DoneCollect(_)     => ???
              case EmitCollect(_)     => ???
              case MergeAll(_, _, _)  => ???
              case FlatMap(_, _)      => ???
              case CatchAll(_, _)     => ???
              case Parallel(_, _)     => ???
            }
          }

          val result = loop[In1, R1, E1, Out, Z](scope.scope, getterRef, env, input, Chunk.empty, self)

          (getterRef.await.map(_.apply()), result)
        }
        .ensuring(scope.close(()))
    }

  def map[Z2](f: Z => Z2): ZConduit[In, R, E, Out, Z2] = self.flatMap(z => ZConduit.succeed(f(z)))

  def mergeMap[In1 <: In, R1 <: R, E1 >: E, Out2](
    f: Out => ZConduit[In1, R1, E1, Out2, Any]
  ): ZConduit[In1, R1, E1, Out2, Any] =
    ZConduit.mergeAll(self.mapOut(f))

  def mergeMapWith[In1 <: In, R1 <: R, E1 >: E, Out2, Z1 >: Z](
    f: Out => ZConduit[In1, R1, E1, Out2, Z1]
  )(z1: Z1)(g: (Z1, Z1) => Z1): ZConduit[In1, R1, E1, Out2, Z1] =
    ZConduit.mergeAllWith(self.mapOut(f))(z1)(g)

  def mapOut[Out2](f: Out => Out2): ZConduit[In, R, E, Out2, Z] = ZConduit.MapOut(self, f)

  def mergeOut[In1 <: In, R1 <: R, E1 >: E, Out2](implicit
    ev: Out <:< ZConduit[In1, R1, E1, Out2, Any]
  ): ZConduit[In1, R1, E1, Out2, Any] =
    ZConduit.mergeAll(self.mapOut(ev))

  def orDie(implicit ev: E <:< Throwable): ZConduit[In, R, Nothing, Out, Z] = orDieWith(ev)

  def orDieWith(f: E => Throwable): ZConduit[In, R, Nothing, Out, Z] = self.catchAll(e => throw f(e))

  def repeated: ZConduit[In, R, E, Out, Nothing] =
    self ++> self.repeated

  def run[R1 <: R, E1 >: E, In1 <: In](
    input: ZIO[R1, E1, Chunk[In1]],
    output: Chunk[Out] => ZIO[R1, E1, Any]
  ): ZIO[R1, E1, Z] =
    launch(input, output).flatMap(_._2)

  def runDrain[R1 <: R, E1 >: E, In1 <: In](input: ZIO[R1, E1, Chunk[In1]]): ZIO[R1, E1, Z] =
    self.run(input, _ => ZIO.unit)

  def unchunk[Out2](implicit ev: Out <:< Chunk[Out2]): ZConduit[In, R, E, Out2, Z] =
    ZConduit
      .concatAllWith(self.map(Option(_)).mapOut(out => ZConduit.emitAll(ev(out)).map(_ => None)))(Option.empty) {
        case (Some(l), _) => Some(l)
        case (_, r)       => r
      }
      .map(_.get)

  def unit: ZConduit[In, R, E, Out, Unit] = self.as(())

  def zip[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, (Z, Z2)] =
    self.flatMap(l => that.map(r => (l, r)))

  def zipLeft[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z] =
    (self zip that).map(_._1)

  def zipPar[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, (Z, Z2)] =
    (ZConduit.identity[In1].mapOut(i => (i, i)) pipeToRight (self &&& that)).mapOut(_.merge)

  def zipParLeft[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z] =
    (self zipPar that).map(_._1)

  def zipParRight[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z2] =
    (self zipPar that).map(_._2)

  def zipRight[In1 <: In, R1 <: R, E1 >: E, Out1 >: Out, Z2](
    that: => ZConduit[In1, R1, E1, Out1, Z2]
  ): ZConduit[In1, R1, E1, Out1, Z2] =
    (self zip that).map(_._2)
}
object ZConduit {
  private[zio] final case class Pipe[InLeft, R, E, Middle, OutRight, LeftZ, RightZ](
    left: () => ZConduit[InLeft, R, E, Middle, LeftZ],
    right: () => ZConduit[Middle, R, E, OutRight, RightZ]
  ) extends ZConduit[InLeft, R, E, OutRight, (LeftZ, RightZ)]
  private[zio] final case class Read[In, R, E, Out, Z](
    more: Chunk[In] => ZConduit[In, R, E, Out, Z],
    done: () => Either[E, Z]
  )                                                                    extends ZConduit[In, R, E, Out, Z]
  private[zio] final case class Done[R, Z](terminal: R => Z)           extends ZConduit[Any, R, Nothing, Nothing, Z]
  private[zio] final case class Halt[E](error: () => Cause[E])         extends ZConduit[Any, Any, E, Nothing, Nothing]
  private[zio] final case class Effect[R, E, Out](zio: ZIO[R, E, Out]) extends ZConduit[Any, R, E, Nothing, Out]
  private[zio] final case class EmitAll[Out](chunk: Chunk[Out])        extends ZConduit[Any, Any, Nothing, Out, Any]
  private[zio] final case class MapOut[In, R, E, Out, Z, Out2](conduit: ZConduit[In, R, E, Out, Z], f: Out => Out2)
      extends ZConduit[In, R, E, Out2, Z]
  private[zio] final case class DoneCollect[In, R, E, Out, Z](conduit: ZConduit[In, R, E, Out, Z])
      extends ZConduit[In, R, E, Nothing, (Chunk[Out], Z)]
  private[zio] final case class EmitCollect[In, R, E, Out, Z](conduit: ZConduit[In, R, E, Out, Z])
      extends ZConduit[In, R, E, (Chunk[Out], Z), Unit]
  private[zio] final case class Ensuring[In, R, E, Out, Z](conduit: ZConduit[In, R, E, Out, Z], v: ZIO[R, Nothing, Any])
      extends ZConduit[In, R, E, Out, Z]
  private[zio] final case class ConcatAll[In, R, E, Out, Out2, Z](
    z: Z,
    f: (Z, Z) => Z,
    value: ZConduit[In, R, E, ZConduit[In, R, E, Out2, Z], Z]
  ) extends ZConduit[In, R, E, Out2, Z]
  private[zio] final case class MergeAll[In, R, E, Out, Out2, Z](
    z: Z,
    f: (Z, Z) => Z,
    value: ZConduit[In, R, E, ZConduit[In, R, E, Out2, Z], Z]
  ) extends ZConduit[In, R, E, Out2, Z]
  private[zio] final case class FlatMap[In, R, E, Out, Done, Done2](
    value: ZConduit[In, R, E, Out, Done],
    f: Done => ZConduit[In, R, E, Out, Done2]
  ) extends ZConduit[In, R, E, Out, Done2]
  private[zio] final case class CatchAll[In, R, E, E1, Out, Z, Done2](
    value: ZConduit[In, R, E, Out, Z],
    f: E => ZConduit[In, R, E1, Out, Done2]
  ) extends ZConduit[In, R, E1, Out, Done2]
  private[zio] final case class Parallel[In, In2, R, E, Out, Out2, Z, Z2](
    left: ZConduit[In, R, E, Out, Z],
    right: ZConduit[In2, R, E, Out2, Z2]
  ) extends ZConduit[(In, In2), R, E, Either[Out, Out2], (Z, Z2)]

  def buffer[In](ref: Ref[Chunk[In]]): ZConduit[In, Any, Nothing, In, Unit] =
    (for {
      chunk  <- readChunk[In]
      middle <- fromEffect(ref.get)
      _      <- emitAll(middle ++ chunk)
    } yield ()).repeated.catchAll(_ => ZConduit.unit)

  def concatAll[In, R, E, Out](
    conduits: ZConduit[In, R, E, ZConduit[In, R, E, Out, Any], Any]
  ): ZConduit[In, R, E, Out, Any] = concatAllWith(conduits)((): Any)((_, _) => ())

  def concatAllWith[In, R, E, Out, Z](
    conduits: ZConduit[In, R, E, ZConduit[In, R, E, Out, Z], Z]
  )(z: Z)(f: (Z, Z) => Z): ZConduit[In, R, E, Out, Z] =
    ConcatAll(z, f, conduits)

  def end[Z](result: => Z): ZConduit[Any, Any, Nothing, Nothing, Z] = Done(_ => result)

  def endWith[R, Z](f: R => Z): ZConduit[Any, R, Nothing, Nothing, Z] = Done(r => f(r))

  def emitAll[Out](all: Chunk[Out]): ZConduit[Any, Any, Nothing, Out, Unit] =
    EmitAll(all).map(_ => ())

  def fail[E](e: => E): ZConduit[Any, Any, E, Nothing, Nothing] = halt(Cause.fail(e))

  def fromEffect[R, E, A](zio: ZIO[R, E, A]): ZConduit[Any, R, E, Nothing, A] = Effect(zio)

  def fromEither[E, A](either: Either[E, A]): ZConduit[Any, Any, E, Nothing, A] =
    either.fold(ZConduit.fail(_), ZConduit.succeed(_))

  def fromOption[A](option: Option[A]): ZConduit[Any, Any, None.type, Nothing, A] =
    option.fold[ZConduit[Any, Any, None.type, Nothing, A]](ZConduit.fail(None))(ZConduit.succeed(_))

  def halt[E](cause: => Cause[E]): ZConduit[Any, Any, E, Nothing, Nothing] = Halt(() => cause)

  def identity[In]: ZConduit[In, Any, Nothing, In, Unit] =
    readChunk[In].flatMap(emitAll(_)).repeated.catchAll(_ => unit)

  def mergeAll[In, R, E, Out](
    conduits: ZConduit[In, R, E, ZConduit[In, R, E, Out, Any], Any]
  ): ZConduit[In, R, E, Out, Any] =
    mergeAllWith(conduits)((): Any)((_, _) => ())

  def mergeAllWith[In, R, E, Out, Z](
    conduits: ZConduit[In, R, E, ZConduit[In, R, E, Out, Z], Z]
  )(z: Z)(f: (Z, Z) => Z): ZConduit[In, R, E, Out, Z] =
    MergeAll(z, f, conduits)

  def readChunkOrFail[E, In](e: E): ZConduit[In, Any, E, Nothing, Chunk[In]] =
    Read(chunk => Done(_ => chunk), () => Left(e))

  def readChunk[In]: ZConduit[In, Any, None.type, Nothing, Chunk[In]] =
    readChunkOrFail(None)

  def succeed[Z](z: => Z): ZConduit[Any, Any, Nothing, Nothing, Z] = end(z)

  val unit: ZConduit[Any, Any, Nothing, Nothing, Unit] = succeed(())
}

class ZStream[-R, +E, +A](val conduit: ZConduit[Unit, R, E, A, Any]) extends AnyVal {
  def flatMap[R1 <: R, E1 >: E, B](f: A => ZStream[R1, E1, B]): ZStream[R1, E1, B] =
    new ZStream(conduit.concatMap(f.andThen(_.conduit)))

  def flatMapPar[R1 <: R, E1 >: E, B](f: A => ZStream[R1, E1, B]): ZStream[R1, E1, B] =
    new ZStream(conduit.mergeMap(f.andThen(_.conduit)))

  def map[B](f: A => B): ZStream[R, E, B] = new ZStream(conduit.mapOut(f))

  def run[R1 <: R, E1 >: E, A1 >: A, Z](sink: ZSink[A1, R1, E1, Z]): ZIO[R1, E1, Z] =
    (conduit pipeToRight sink.conduit).runDrain(ZIO.succeed(Chunk.empty))

  def transduce[R1 <: R, E1 >: E, A1 >: A, Z](sink: ZSink[A1, R1, E1, Z]): ZStream[R1, E1, Z] =
    new ZStream(ZConduit.fromEffect(Ref.make[Chunk[A1]](Chunk.empty)).flatMap { ref =>
      val buffer = ZConduit.buffer[A1](ref)

      conduit >>>
        buffer >>>
        (for {
          tuple        <- sink.conduit.doneCollect
          (leftover, z) = tuple
          _            <- ZConduit.fromEffect(ref.set(leftover))
        } yield z).repeated
    })

  def toPull[R, E, A]: ZManaged[R, Nothing, ZIO[R, Option[E], A]] = ???
}
object ZStream {
  def fromPull[R, E, A](zio: ZManaged[R, Nothing, ZIO[R, Option[E], A]]): ZStream[R, E, A] = ???
}

class ZSink[In, -R, +E, +Z](val conduit: ZConduit[In, R, E, In, Z]) extends AnyVal { self =>
  def toDriver: ZManaged[R, Nothing, Option[In] => ZIO[R, E, Option[(Chunk[In], Z)]]] = ???

  def >>>[R1 <: R, E1 >: E, Z1 >: Z, Z2](that: => ZSink[Z1, R1, E1, Z2]): ZSink[In, R1, E1, Z2] =
    new ZSink(
      (self.conduit.emitCollect pipeToRight
        (ZConduit.identity[Chunk[In]] &&& that.conduit).map(_._2)).doneCollect.flatMap { case (chunk, z2) =>
        ZConduit.emitAll(chunk.collect { case Left(chunk) => chunk }.flatten).map(_ => z2)
      }
    )
}

class DelayedRef[A](data: Ref[DelayedRef.State[A]]) {
  import DelayedRef.State._

  def await: UIO[A] =
    data.get.flatMap {
      case Unset(p) => p.await *> await
      case Set(a)   => UIO.succeed(a)
    }

  def set(a: A): UIO[Any] =
    data.modify {
      case Unset(p) => (p.succeed(()), Set(a))
      case Set(_)   => (UIO.unit, Set(a))
    }.flatten.uninterruptible
}

object DelayedRef {
  sealed abstract class State[A]
  object State {
    case class Unset[A](p: Promise[Nothing, Unit]) extends State[A]
    case class Set[A](ref: A)                      extends State[A]
  }

  def make[A]: UIO[DelayedRef[A]] =
    Promise.make[Nothing, Unit].flatMap(p => Ref.make(State.Unset[A](p): State[A])).map(new DelayedRef(_))
}
