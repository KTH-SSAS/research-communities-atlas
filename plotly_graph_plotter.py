import plotly.graph_objects as go
import networkx as nx
import random

class PlotlyGraphPlotter:
    def __init__(self, name, graph, pos, node_sizes, edge_widths, edge_list, node_names):
        self.name = name
        self.graph = graph
        self.pos = pos
        self.node_sizes = node_sizes
        self.edge_widths = edge_widths
        self.edge_list = edge_list # This is only needed to get the proper widht for each edge. The edges themselves are taken from the graph.
        self.node_names = node_names
        self.plot_graph()
    
    def plot_graph(self):
        G = self.graph
        edge_x = []
        edge_y = []
        edge_trace = []

        for edge in G.edges():
            x0, y0 = self.pos[edge[0]]
            x1, y1 = self.pos[edge[1]]
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None)

            edg_trc = go.Scatter(
                x=(x0, x1, None), y=(y0, y1, None),
                line=dict(width=self.edge_widths[self.edge_list.index((edge[0],edge[1]))]*0.5, color='#888'),
                hoverinfo='none',
                mode='lines')
            edge_trace.append(edg_trc)

        # edge_trace = go.Scatter(
        #     x=edge_x, y=edge_y,
        #     line=dict(width=0.5, color='#888'),
        #     hoverinfo='none',
        #     mode='lines')

        node_x = []
        node_y = []
        for node in G.nodes():
            x, y = self.pos[node]
            node_x.append(x)
            node_y.append(y)

        colors = ["DeepSkyBlue", "LightSkyBlue", "LightSteelBlue", "LightBlue", "PaleTurquoise", "DarkTurquoise",
                       "cyan", "LightCyan", "CadetBlue", "MediumAquamarine",
                       "aquamarine", "DarkSeaGreen", "SeaGreen", "MediumSeaGreen",
                       "LightSeaGreen", "PaleGreen", "SpringGreen", "LawnGreen", "green", "chartreuse",
                       "MediumSpringGreen", "GreenYellow", "LimeGreen", "YellowGreen", "OliveDrab",
                       "DarkKhaki", "khaki", "PaleGoldenrod", "LightGoldenrodYellow", "LightYellow", "yellow", "gold",
                       "goldenrod", "DarkGoldenrod", "RosyBrown", "IndianRed",
                       "sienna", "peru", "burlywood", "beige", "wheat", "SandyBrown", "tan", "chocolate",
                       "DarkSalmon", "salmon", "LightSalmon", "DarkOrange", "coral", "LightCoral",
                       "tomato", "OrangeRed", "red", "HotPink", "DeepPink", "pink", "LightPink", "PaleVioletRed",
                       "MediumVioletRed", "magenta", "violet", "plum", "orchid", "MediumOrchid"]
        random.shuffle(colors) # Shuffle the colors to use then in a random order

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            marker=dict(
                showscale=False,
                # colorscale options
                #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
                #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
                #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
                # colorscale='YlGnBu',
                # reversescale=True,
                color=colors,
                size=10,
                colorbar=dict(
                    thickness=15,
                    title='Node Connections',
                    xanchor='left',
                    titleside='right'
                ),
                line_width=2),
            textposition="top center",
            textfont=dict(
                color="black"
            )
        )

        node_adjacencies = []
        for node, adjacencies in enumerate(G.adjacency()):
            node_adjacencies.append(len(adjacencies[1]))

        # node_trace.marker.color = self.node_sizes
        node_trace.marker.size = self.node_sizes
        node_trace.text = list(self.node_names.values())

        # First add the node_trace
        fig = go.Figure(data=[node_trace],
             layout=go.Layout(
                title=self.name,
                titlefont_size=16,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                )
        # Then add the edge_traces
        for trace in edge_trace:
            fig.add_trace(trace)
        fig.show()
