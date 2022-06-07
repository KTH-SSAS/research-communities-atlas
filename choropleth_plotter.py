import plotly.graph_objects as go
import pandas as pd
from pycountry_convert import country_name_to_country_alpha3

class ChoroplethPlotter:
    def __init__(self, name, data, complete_data=False):
        self.name = name
        self.data = data
        self.plot_choropleth(complete_data)

    def plot_choropleth(self, complete_data=False):
        data2 = []
        for country in self.data:
            try:
                country_code = country_name_to_country_alpha3(country[0])
                data2.append(country_code)
            except KeyError:
                data2.append('unknown_country')
                continue

        if not complete_data:
            df = pd.DataFrame(self.data, columns =['Name', 'Articles'])
            df.insert(2, "Code", data2, True)
            # Export also the dataframe to a csv for future reuse
            df.to_csv(self.name.lower().replace(' ', '_') + '.csv')
        else:
            df = self.data

        fig = go.Figure(data=go.Choropleth(
            locations = df['Code'],
            z = df['Articles'],
            text = df['Name'],
            colorscale = 'RdBu',
            autocolorscale=False,
            reversescale=True,
            marker_line_color='darkgray',
            marker_line_width=0.5,
            colorbar_tickprefix = '',
            colorbar_title = '# of articles'
        ))

        # Uncomment this to show article nubmers on top of countries
        # fig.add_trace(go.Scattergeo(
        #     locations = df['Code'],
        #     text = df['Articles'],
        #     mode="text",
        #     textposition="middle center",
        #     textfont=dict(
        #         size=8,
        #         color="white"
        #     )
        # ))

        fig.update_layout(
            title_text=self.name,
            geo=dict(
                showframe=False,
                showcoastlines=True,
                projection_type='equirectangular',
                showland=False,
                showlakes=False,
                showrivers=False
            ),
            annotations = [dict(
                x=0.55,
                y=0.1,
                xref='paper',
                yref='paper',
                text='',
                showarrow = False
            )]
        )

        # hover_scatter = [scatt for scatt in fig.data if scatt.mode == 'markers'][0]
        # # Show text
        # hover_scatter.mode = 'markers+text'

        # # Set font properties
        # hover_scatter.textfont.size = 8

        fig.show()
        # fig.write_image("fig1.png")

# data = [('United States', 2341), ('China', 1144), ('India', 565), ('United Kingdom', 281), ('South Korea', 270), ('Canada', 238), ('Germany', 217), ('Italy', 206), ('Australia', 201), ('Taiwan', 200), ('Spain', 179), ('France', 168), ('Malaysia', 153), ('Brazil', 140), ('Japan', 126)]
# data = pd.read_csv('global_articles_produced_by_each_country.csv')
# ChoroplethPlotter("Articles produced by each country", data, complete_data=True)
