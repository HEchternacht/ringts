"""
Analytics functions for generating graphs and statistics.
"""
import gc
import pandas as pd
import plotly.graph_objects as go


def get_delta_between(datetime1, datetime2, database):
    """Filter deltas between two datetimes"""
    table = database.get_deltas()
    datetime1 = pd.to_datetime(datetime1)
    datetime2 = pd.to_datetime(datetime2)
    mask = (table['update time'] >= datetime1) & (table['update time'] <= datetime2)
    return table[mask]


def preprocess_vis_data(all_update_times, all_player_data, names_list):
    """
    Preprocess visualization data to compress consecutive zero periods.
    Returns compressed times and player data with zeros grouped.
    """
    num_times = len(all_update_times)
    
    # Identify positions where ALL players have zero
    all_zero_positions = []
    for i in range(num_times):
        if all(all_player_data[name][i] == 0 for name in names_list):
            all_zero_positions.append(i)
    
    # Group consecutive zero positions (only if there are 2+ consecutive zeros)
    zero_groups = []
    if all_zero_positions:
        start = all_zero_positions[0]
        for i in range(1, len(all_zero_positions)):
            if all_zero_positions[i] != all_zero_positions[i-1] + 1:
                # End of a consecutive group
                if all_zero_positions[i-1] - start >= 1:  # At least 2 consecutive zeros
                    zero_groups.append((start, all_zero_positions[i-1]))
                start = all_zero_positions[i]
        # Don't forget the last group
        if all_zero_positions[-1] - start >= 1:
            zero_groups.append((start, all_zero_positions[-1]))
    
    # Build compressed timeline - convert everything to strings with smart date display
    compressed_times = []
    compressed_data = {name: [] for name in names_list}
    
    prev_date = None
    prev_time = None
    i = 0
    while i < num_times:
        # Check if this position starts a zero group
        in_zero_group = False
        for start, end in zero_groups:
            if i == start:
                # Create label for zero period
                # Use timestamp BEFORE the zero group starts (if it exists)
                if start > 0:
                    start_time = pd.to_datetime(all_update_times[start - 1])
                else:
                    start_time = pd.to_datetime(all_update_times[start])
                end_time = pd.to_datetime(all_update_times[end])
                
                # Format with smart date display
                start_date = start_time.date()
                end_date = end_time.date()
                
                if start_date == end_date:
                    # Same day - show date once
                    if start_date != prev_date:
                        label = f"{start_time.strftime('%d/%m/%Y %H:%M')}->{end_time.strftime('%H:%M')}"
                    else:
                        label = f"{start_time.strftime('%H:%M')}->{end_time.strftime('%H:%M')}"
                else:
                    # Different days - show both dates
                    if start_date != prev_date:
                        label = f"{start_time.strftime('%d/%m/%Y %H:%M')}->{end_time.strftime('%d/%m/%Y %H:%M')}"
                    else:
                        label = f"{start_time.strftime('%H:%M')}->{end_time.strftime('%d/%m/%Y %H:%M')}"
                
                compressed_times.append(label)
                prev_date = end_date
                prev_time = end_time
                
                # Add single zero for each player for this period
                for name in names_list:
                    compressed_data[name].append(0)
                
                i = end + 1
                in_zero_group = True
                break
        
        if not in_zero_group:
            # Regular data point - show as range from previous timestamp to current
            time_obj = pd.to_datetime(all_update_times[i])
            current_date = time_obj.date()
            
            # Determine start time for this bucket (previous timestamp or first point)
            if prev_time is None:
                # First data point - show as single time point
                if current_date != prev_date:
                    time_str = time_obj.strftime('%d/%m/%Y %H:%M')
                else:
                    time_str = time_obj.strftime('%H:%M')
            else:
                # Show range from previous timestamp to current
                start_date = prev_time.date()
                
                if start_date == current_date:
                    # Same day - show time range
                    if current_date != prev_date:
                        time_str = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%H:%M')}"
                    else:
                        time_str = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%H:%M')}"
                else:
                    # Different days - show full range
                    if start_date != prev_date:
                        time_str = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                    else:
                        time_str = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
            
            compressed_times.append(time_str)
            prev_date = current_date
            prev_time = time_obj
            
            for name in names_list:
                compressed_data[name].append(all_player_data[name][i])
            i += 1
    
    del all_zero_positions, zero_groups
    gc.collect()
    return compressed_times, compressed_data


def create_interactive_graph(names, database, datetime1=None, datetime2=None):
    """Create interactive Plotly graph for player EXP gains"""
    # Custom color palette based on theme colors
    theme_colors = [
        '#C21500',  # Primary red-orange
        '#FFC500',  # Primary golden yellow
        '#FF6B35',  # Complementary orange
        '#FFE156',  # Light yellow
        '#B81400',  # Darker red
        '#E6A900',  # Darker yellow
        '#FF8F66',  # Light orange
        '#FFD966',  # Pale yellow
    ]
    
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Handle single name or list of names
    names_list = [names] if isinstance(names, str) else names

    # Get all unique update times across all data (standardized timeline)
    all_update_times = sorted(table['update time'].unique())

    # Build data for all players first
    all_player_data = {}
    for name in names_list:
        player_data = table[table['name'] == name]
        if not player_data.empty:
            player_deltas = dict(zip(player_data['update time'], player_data['deltaexp']))
            standardized_exps = [player_deltas.get(update_time, 0) for update_time in all_update_times]
            all_player_data[name] = standardized_exps

    if not all_player_data:
        fig = go.Figure()
        fig.update_layout(title='No data available')
        return fig.to_json()

    # Preprocess data to compress zero periods
    compressed_times, compressed_data = preprocess_vis_data(all_update_times, all_player_data, names_list)

    # Create plotly figure
    fig = go.Figure()

    # Build traces with compressed data
    for idx, name in enumerate(names_list):
        color = theme_colors[idx % len(theme_colors)]
        # Add bar trace
        fig.add_trace(go.Bar(
            x=compressed_times,
            y=compressed_data[name],
            name=name,
            marker_color=color,
            text=[str(int(exp)) if exp > 0 else '' for exp in compressed_data[name]],
            textposition='outside',
            textangle=0,
            hovertemplate='<b>%{x}</b><br>EXP: %{y:,.0f}<extra></extra>'
        ))
        
        # Add smooth line connecting the tops of the bars
        fig.add_trace(go.Scatter(
            x=compressed_times,
            y=compressed_data[name],
            name=f'{name} (trend)',
            mode='lines',
            line=dict(color=color, width=2, shape='spline'),
            showlegend=False,
            hoverinfo='skip'
        ))

    fig.update_layout(
        title=f'EXP Gain Over Time',
        xaxis_title='Update Time',
        yaxis_title='Delta EXP',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True,
        barmode='group',  # Group bars side by side for multiple players
        bargap=0,  # No gap between bars
        bargroupgap=0,  # No gap between bar groups
        xaxis=dict(
            type='category',  # Treat x-axis as categorical (string bins)
            tickangle=-45,
            tickmode='auto',
            nticks=20
        ),
        colorway=theme_colors  # Set default color palette
    )

    result = fig.to_json()
    del table, all_update_times, all_player_data, compressed_times, compressed_data, fig
    gc.collect()
    return result


def get_player_stats(names, database, datetime1=None, datetime2=None):
    """Get statistics table for players"""
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Filter by names if specified
    if names:
        names_list = [names] if isinstance(names, str) else names
        table = table[table['name'].isin(names_list)]

    # Group and calculate stats
    stats = table.groupby('name').agg({
        'deltaexp': ['sum', 'mean', 'count', 'max', 'min']
    }).round(2)

    stats.columns = ['Total EXP', 'Average EXP', 'Updates', 'Max EXP', 'Min EXP']
    stats = stats.sort_values('Total EXP', ascending=False)
    stats = stats.reset_index()

    result = stats.to_dict('records')
    del table, stats
    gc.collect()
    return result
