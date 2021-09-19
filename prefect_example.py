from prefect import task, Flow


# since its a task call the task decorator
@task
def create_list():
    return [1, 1, 2, 3]


# since its a task call the task decorator
@task
def add_one(x):
    return x + 1


# since its a task call the task decorator
@task
def get_sum(x):
    return sum(x)


# Now let's group them together in the Flow
with Flow("simple-map") as f:
    plus_one = add_one.map(create_list)
    plus_two = add_one.map(plus_one)
    result = get_sum(plus_two)
    # f.visualize()
    # f.run()
f.register(project_name="prefect_example")
