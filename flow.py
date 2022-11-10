from prefect import flow, task
from typing import List
import httpx
import time


@task(retries=3)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@task(retries=3)
def sample_task(repo: str):
    time.sleep(0.5)
    print(f"{repo} second task")


@flow(name="GitHub Stars")
def github_stars(repos: List[str]):
    for repo in repos:
        get_stars.submit(repo)
        sample_task.submit(repo)


# run the flow!
github_stars(["PrefectHQ/Prefect", "abhr1994/DuckDB_Datalake"])

# import time
# from prefect import task, flow
#
# @task
# def print_values(values):
#     for value in values:
#         time.sleep(0.5)
#         print(value, end="\r")
#
# @flow
# def my_flow():
#     print_values.submit(["AAAA"] * 15)
#     print_values.submit(["BBBB"] * 10)
#
# if __name__ == "__main__":
#     my_flow()
