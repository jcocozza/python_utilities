import jinja2


def jinja_render(file_path: str, **kwargs) -> str:
    """
    Render a parameterized file

    :param file_path: path of the file
    :param **kwargs: The named parameters of the file(and their values)

    :returns: a string of rendered file where the variables are interpolated with their passed values
    """
    with open(file_path) as file:
        file_str = file.read()

        template = jinja2.Template(file_str)

        rendered_query = template.render(**kwargs)
        return rendered_query