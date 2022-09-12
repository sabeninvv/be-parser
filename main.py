from src import *


if os_name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def run_simplenote_parser(queue: Queue):
    SimplenoteParser(queue=queue).fill_redis_with_matches()


def run_betexplorer_parser(queue: Queue):
    asyncio.run(
        BetExplorerParser(queue=queue).ride_round_robin()
    )


if __name__ == "__main__":
    queue = Queue()
    processes = []
    for process in [
        Process(target=run_simplenote_parser, args=(queue,)),
        Process(target=run_betexplorer_parser, args=(queue,))
    ]:
        process.daemon = True
        processes.append(process)

    _ = [process.start() for process in processes]
    _ = [process.join() for process in processes]
