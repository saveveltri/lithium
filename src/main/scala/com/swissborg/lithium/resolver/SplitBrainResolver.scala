package com.swissborg.lithium

package resolver

import akka.actor._
import akka.cluster._
import cats.effect.SyncIO
import cats.implicits._
import com.swissborg.lithium.reporter._
import com.swissborg.lithium.strategy._

import scala.concurrent.duration._

/**
 * Actor resolving split-brain scenarios.
 *
 * It handles two events: `SBResolver.HandleSplitBrain` and `SBResolver.DownAll`,
 * both accompagnied with a `WorldView` describing a partitioned cluster.
 *
 * The `Resolver.HandleSplitBrain` event triggers the downing of the members
 * as described by the decision given by `Union(_strategy, IndirectlyConnected)`.
 *
 * The `Resolver.DownAll` event triggers the downing of all the nodes.
 *
 * @param _strategy                     the strategy with which to resolved the split-brain.
 * @param stableAfter                   duration during which a cluster has to be stable before attempting to resolve a split-brain.
 * @param downAllWhenUnstable           down the partition if the cluster has been unstable for longer than `stableAfter + 3/4 * stableAfter`.
 * @param trackIndirectlyConnectedNodes downs the detected indirectly-connected nodes when enabled.
 */
private[lithium] class SplitBrainResolver(private val _strategy: Strategy[SyncIO],
                                          private val stableAfter: FiniteDuration,
                                          private val downAllWhenUnstable: Option[FiniteDuration],
                                          private val trackIndirectlyConnectedNodes: Boolean)
    extends Actor
    with ActorLogging {
  context.actorOf(SplitBrainReporter.props(self, stableAfter, downAllWhenUnstable, trackIndirectlyConnectedNodes),
                  "split-brain-reporter")

  private val cluster: Cluster                 = Cluster(context.system)
  private val selfUniqueAddress: UniqueAddress = cluster.selfUniqueAddress

  private val strategy: Union[SyncIO, Strategy, IndirectlyConnected] =
    new Union(_strategy, new IndirectlyConnected)

  private val downAll: DownAll[SyncIO] = new DownAll()

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def receive: Receive = {
    case SplitBrainResolver.ResolveSplitBrain(worldView) =>
      resolveSplitBrain(worldView).unsafeRunSync()

    case SplitBrainResolver.DownAll(worldView) =>
      downAll(worldView).unsafeRunSync()
  }

  /**
   * Handle the partition using the [[Union]] of the configured
   * strategy and the [[IndirectlyConnected]].
   */
  private def resolveSplitBrain(worldView: WorldView): SyncIO[Unit] =
    for {
      _ <- SyncIO(
        log.info(
          """[{}] Received request to handle a split-brain...
            |-- Worldview --
            |Reachable nodes:
            |  {}
            |Unreachable nodes:
            |  {}
            |Indirectly-connected nodes:
            |  {}
            |""".stripMargin,
          selfUniqueAddress,
          worldView.reachableNodes.mkString_("\n  "),
          worldView.unreachableNodes.mkString_("\n  "),
          worldView.indirectlyConnectedNodes.mkString_("\n  ")
        )
      )
      _ <- runStrategy(strategy, worldView)
    } yield ()

  /**
   * Handle the partition by downing all the members.
   */
  private def downAll(worldView: WorldView): SyncIO[Unit] =
    for {
      _ <- SyncIO(
        log.info(
          """[{}] Received request to down all the nodes...
            |-- Worldview --
            |Reachable nodes:
            |  {}
            |Unreachable nodes:
            |  {}
            |Indirectly-connected nodes:
            |  {}
            |""".stripMargin,
          selfUniqueAddress,
          worldView.reachableNodes.mkString_("\n  "),
          worldView.unreachableNodes.mkString_("\n  "),
          worldView.indirectlyConnectedNodes.mkString_("\n  ")
        )
      )
      _ <- runStrategy(downAll, worldView)
    } yield ()

  /**
   * Run `strategy` on `worldView`.
   *
   * Enable `nonJoiningOnly` so that joining and weakly-up
   * members do not run the strategy.
   */
  private def runStrategy(strategy: Strategy[SyncIO], worldView: WorldView): SyncIO[Unit] = {
    def execute(decision: Decision): SyncIO[Unit] =
      for {
        _ <- SyncIO(
          log.info("""[{}] Downing the nodes:
                     |  {}""".stripMargin, selfUniqueAddress, Decision.allNodesToDown(decision).mkString_("\n  "))
        )
        _ <- decision.nodesToDown.toList.traverse_(node => SyncIO(cluster.down(node.address)))

      } yield ()

    strategy
      .takeDecision(worldView)
      .flatMap(execute)
      .handleErrorWith(
        err => SyncIO(log.error(err, "[{}] An error occurred during the resolution.", selfUniqueAddress))
      )
  }
}

object SplitBrainResolver {

  def props(strategy: Strategy[SyncIO],
            stableAfter: FiniteDuration,
            downAllWhenUnstable: Option[FiniteDuration],
            trackIndirectlyConnectedNodes: Boolean): Props =
    Props(new SplitBrainResolver(strategy, stableAfter, downAllWhenUnstable, trackIndirectlyConnectedNodes))

  sealed trait Event

  final case class ResolveSplitBrain(worldView: WorldView) extends Event

  final case class DownAll(worldView: WorldView) extends Event

}
