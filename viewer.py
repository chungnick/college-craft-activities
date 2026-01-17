import matplotlib.pyplot as plt
import pandas as pd
import io
import re

# Raw data
data = """School	2025	 2024 	2023	2022	2021	2020	2019	2018
Amherst College	7.72%	9.01%	9.82%	7.26%	8.74%	11.83%	9.51%	12.81%
Babson College	16.00%	17.09%	19.74%	22.35%	25.35%	26.97%	26.41%	24.41%
Barnard College	10.05%	8.84%	7.96%	8.79%	11.47%	13.60%	11.77%	13.92%
Boston College	13.85%	16.19%	15.65%	16.66%	19.04%	26.38%	27.22%	27.89%
Boston University	12.83%	11.11%	10.85%	14.37%	18.65%	20.09%	18.94%	22.09%
Bowdoin College	6.81%	7.13%	8.02%	9.19%	8.82%	9.16%	9.05%	10.26%
Brown University	5.65%	5.39%	5.23%	5.06%	5.51%	7.67%	7.07%	7.67%
Bucknell University	32.20%	28.93%	 32.01%	32.55%	34.50%	34.09%	34.23%	33.04%
California Institute of Technology	3.78%	2.57%	3.14%	2.69%	3.92%	6.69%	6.42%	6.62%
Carleton College	20.00%	20.41%	 22.28%	16.63%	17.55%	20.28%	18.97%	19.84%
Carnegie Mellon University	11.07%	11.66%	11.40%	11.30%	13.54%	17.27%	15.44%	17.12%
Claremont McKenna College	9.44%	9.59%	11.12%	10.35%	11.24%	13.34%	10.30%	9.31%
Colby College	8.00%	7.09%	6.83%	7.61%	8.87%	10.27%	9.67%	13.06%
Colgate University	17.43%	13.88%	11.95%	12.43%	17.19%	27.47%	22.58%	24.93%
College of William & Mary	36.96%	In-state: 36.32% Out-of-state: 34.43%	In-state: 39.28% Out-of-state: 28.19%	In-state: 41.86% Out-of-state: 28.10%	In-state: 43.98% Out-of-state: 31.45%	In-state: 51.29% Out-of-state: 35.53%	In-state: 46.94% Out-of-state: 30.99%	In-state: 45.12% Out-of-state: 30.90%
Columbia University	4.94%	3.86%	4.00%	3.74%	3.89%	6.66%	5.45%	5.91%
Cornell University	8.38%	8.41%	7.90%	7.47%	8.69%	10.71%	10.85%	10.61%
Dartmouth College	6.02%	5.40%	6.23%	6.38%	6.17%	9.22%	7.93%	8.74%
Davidson College	12.60%	13.37%	 14.48%	16.95%	17.80%	20.00%	18.05%	19.50%
Duke University	5.20%	5.71%	6.78%	6.35%	5.88%	7.79%	7.60%	8.91%
Emory University	Emory: 10.28% Oxford: 13.35%	Emory: 10.29% Oxford: 12.66%	Emory: 10.65% Oxford: 17.25%	Emory: 11.35% Oxford: 15.00%	Emory: 13.05% Oxford: 19.82%	Emory: 19.17% Oxford: 24.17%	Emory: 15.60% Oxford: 19.83%	Emory: 18.52% Oxford: 24.95%
George Washington University	TBA	47.09%	43.54%	48.98%	49.71%	43.04%	40.84%	41.87%
Georgetown University	12.00%	12.91%	13.08%	12.23%	11.98%	16.81%	14.36%	14.52%
Georgia Institute of Technology	In-state: 29.47% Out-of-state: 9.64%	In-state: 33.13% Out-of-state: 9.93%	In-state: 36.60% Out-of-state: 12.03%	In-state: 35.48% Out-of-state: 13.04%	In-state: 33.57% Out-of-state: 14.69%	In-state: 39.51% Out-of-state: 17.72%	In-state: 40.26% Out-of-state: 16.25%	In-state: 37.59% Out-of-state: 19.17%
Hamilton College	13.59%	13.62%	11.77%	11.78%	14.07%	18.41%	16.39%	21.28%
Harvard University	4.18%	3.65%	3.45%	3.24%	4.01%	5.01%	4.64%	4.73%
Harvey Mudd College	12.33%	12.66%	13.06%	13.36%	9.99%	17.96%	13.67%	14.48%
Haverford College	13.33%	12.37%	12.91%	14.21%	17.84%	18.23%	16.32%	18.79%
Johns Hopkins University	5.14%	6.44%	 7.56%	7.25%	7.52%	11.06%	11.17%	11.48%
Massachusetts Institute of Technology	4.56%	4.55%	4.80%	3.96%	4.11%	7.26%	6.70%	6.74%
Middlebury College	14.91%	10.75%	 10.37%	 12.69%	13.45%	22.04%	15.36%	16.71%
New York University	7.70%	9.23%	9.41%	12.46%	12.96%	21.09%	16.20%	19.99%
Northwestern University	7.00%	7.69%	7.22%	7.21%	6.97%	9.31%	9.05%	8.46%
Pomona College	6.90%	7.09%	6.76%	7.02%	6.64%	8.62%	7.40%	7.61%
Princeton University	4.42%	4.62%	4.50%	5.70%	4.38%	5.63%	5.78%	5.48%
Rice University	8.01%	8.19%	7.88%	8.68%	9.48%	10.89%	8.72%	11.13%
Stanford University	TBA	3.61%	3.91%	 3.68%	3.95%	5.19%	4.34%	4.36%
Swarthmore College	7.52%	7.47%	6.94%	6.93%	7.79%	9.06%	8.93%	9.49%
Trinity University	24.88%	25.92%	28.16%	30.51%	33.72%	33.62%	28.76%	34.16%
Tufts University	10.81%	11.49%	10.13%	9.69%	11.43%	16.30%	14.95%	14.62%
Tulane University	14.46%	13.98%	 14.59%	11.45%	9.63%	11.11%	12.87%	17.32%
University of California. Berkeley	In-state: 13.59% Out-of-state: 8.45%	In-state: 14.93% Out-of-state: 5.64%	In-state: 15.14% Out-of-state: 7.07%	In-state: 14.47% Out-of-state: 7.25%	In-state: 16.81% Out-of-state: 11.54%	In-state: 20.17% Out-of-state: 13.89%	In-state: 18.62% Out-of-state: 13.26%	In-state: 16.87% Out-of-state: 12.05%
University of California, Los Angeles	In-state: 9.60% Out-of-state: 9.12%	In-state: 9.52% Out-of-state: 8.02%	In-state: 9.46% Out-of-state: 7.53%	In-state: 9.20% Out-of-state: 7.59%	In-state: 10.02% Out-of-state: 11.92%	In-state: 13.55% Out-of-state: 15.63%	In-state: 12.00% Out-of-state: 12.87%	In-state: 12.19% Out-of-state: 17.17%
University of Chicago	TBA	4.48%	 4.79%	5.43%	6.48%	7.31%	6.17%	7.26%
University of Michigan	16.42%	15.64%	17.94%	17.69%	20.15%	26.11%	22.91%	22.83%
University of North Carolina at Chapel Hill	In-state: 34.32% Out-of-state: 10.88%	In-state: 37.99% Out-of-state: 7.84%	In-state: 41.16% Out-of-state: 10.67%	In-state: 42.31% Out-of-state: 8.71%	In-state: 42.58% Out-of-state: 11.58%	In-state: 48.35% Out-of-state: 14.73%	In-state: 42.53% Out-of-state: 13.86%	In-state: 41.27% Out-of-state: 13.83%
University of Notre Dame	9.00%	11.27%	12.38%	12.91%	15.07%	18.99%	15.83%	17.71%
University of Pennsylvania	4.87%	5.40%	5.87%	6.50%	5.87%	8.98%	7.66%	8.41%
University of Richmond	22.24%	22.20%	23.31%	24.37%	28.77%	30.90%	28.33%	30.17%
University of Southern California	11.19%	9.81%	10.01%	12.02%	12.51%	16.11%	11.42%	12.96%
University of Virginia	In-state: 23.50% Out-of-state: 12.69%	In-state: 25.93% Out-of-state: 13.27%	In-state: 27.58% Out-of-state: 12.92%	In-state: 27.63% Out-of-state: 15.15%	In-state: 28.73% Out-of-state: 17.45%	In-state: 35.47% Out-of-state: 17.14%	In-state: 36.20% Out-of-state: 18.83%	In-state: 37.97% Out-of-state: 21.37%
Vanderbilt University	5.33%	5.86%	6.28%	6.67%	7.14%	11.62%	9.12%	9.61%
Vassar College	TBA	18.57%	17.73%	18.66%	20.15%	24.54%	23.74%	24.58%
Wake Forest University	TBA	21.67%	21.56%	21.37%	25.18%	31.98%	29.30%	29.40%
Washington and Lee University	13.56%	13.97%	 17.36%	16.96%	18.87%	24.51%	18.57%	21.16%
Washington University in St. Louis	11.92%	12.06%	11.96%	11.76%	13.00%	16.02%	13.85%	15.03%
Wellesley College	14.79%	14.05%	13.91%	13.57%	16.18%	20.41%	21.56%	19.54%
Wesleyan University	16.11%	16.49%	17.10%	14.43%	19.44%	20.90%	16.48%	17.46%
Williams College	8.50%	8.25%	9.99%	8.50%	8.83%	15.12%	12.60%	12.97%
Yale University	4.75%	3.87%	4.50%	4.57%	5.31%	6.53%	6.08%	6.35%"""

def parse_percentage(value):
    # Handle TBA or empty
    if not value or 'TBA' in value:
        return None
        
    # Handle cases with multiple values (In-state/Out-of-state/Emory/Oxford)
    # We'll take the average if multiple percentages found, or just the first valid one if simple
    # Regex to find percentages
    percentages = re.findall(r'(\d+\.?\d*)%', value)
    
    if not percentages:
        return None
        
    # Convert to floats
    floats = [float(p) for p in percentages]
    
    # Calculate average
    return sum(floats) / len(floats)

def parse_data(raw_data):
    lines = [line.strip() for line in raw_data.strip().split('\n')]
    header = re.split(r'\t+', lines[0])
    years = [int(y.strip()) for y in header[1:]]
    
    parsed_data = []
    
    for line in lines[1:]:
        parts = re.split(r'\t+', line)
        school = parts[0]
        rates = []
        
        for i, val in enumerate(parts[1:]):
            if i < len(years):
                rate = parse_percentage(val)
                rates.append(rate)
                
        parsed_data.append({
            'School': school,
            'Rates': rates
        })
        
    return years, parsed_data

def create_chart(years, data):
    plt.figure(figsize=(15, 10))
    
    # Calculate yearly averages
    yearly_rates = {y: [] for y in years}
    
    for entry in data:
        school = entry['School']
        rates = entry['Rates']
        
        # Filter None values for plotting
        valid_points = [(y, r) for y, r in zip(years, rates) if r is not None]
        if valid_points:
            x_vals, y_vals = zip(*valid_points)
            plt.plot(x_vals, y_vals, marker='o', label=school, alpha=0.3, linewidth=1) # Reduced alpha/width for individual schools
            
            # Collect for average
            for y, r in valid_points:
                yearly_rates[y].append(r)

    # Plot Average Trend Line
    avg_x = []
    avg_y = []
    for year in sorted(years):
        rates_list = yearly_rates.get(year, [])
        if rates_list:
            avg_x.append(year)
            avg_y.append(sum(rates_list) / len(rates_list))
            
    if avg_x:
        plt.plot(avg_x, avg_y, color='black', linewidth=4, marker='o', label='AVERAGE TREND', zorder=10)

    plt.title('College Acceptance Rates (2018-2025)', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Acceptance Rate (%)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Reverse x-axis to show 2018 -> 2025 if preferred, or keep chronological 
    # Current data header is 2025 -> 2018. 
    # Plotting x_vals will put them on numeric axis (2018...2025) correctly automatically.
    
    # Legend might be too big. Put it outside.
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0., fontsize='small', ncol=2)
    
    plt.tight_layout()
    plt.savefig('acceptance_rates.png', dpi=300)
    print("Chart saved to acceptance_rates.png")

if __name__ == "__main__":
    years, parsed = parse_data(data)
    create_chart(years, parsed)

