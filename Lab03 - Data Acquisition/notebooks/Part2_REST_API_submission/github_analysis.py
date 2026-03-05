# Task 1 
# 1.1
def task1_fetch_repos():
    """
    Fetch repository information for major ML frameworks.
    Returns a DataFrame with key metrics.
    """
    api = GitHubAPI(token=os.getenv('GITHUB_TOKEN')) 
    repos = ['tensorflow/tensorflow', 'pytorch/pytorch', 'scikit-learn/scikit-learn']
    results = []
    for path in repos: 
        owner, repo_name = path.split('/')
        try: 
            repo_data = api.get_repo(owner, repo_name)
            results.append({
                'name': repo_data['name'],
                'stars': repo_data['stargazers_count'],
                'forks': repo_data['forks_count'],
                'language': repo_data['language'],
                'open_issues': repo_data['open_issues_count'],
                'created_date': repo_data['created_at'],
            })
        except Exception as e:
            print(f"Error fetching {path}:{e}")
    df = pd.DataFrame(results)
    return df

df = task1_fetch_repos()
df.to_csv('task1_github.csv', index=False)


# 1.2 Metrics 
df['created_date'] = pd.to_datetime(df['created_date'])

now = datetime.now(df['created_date'].dt.tz)
df['age_days'] = (now - df['created_date']).dt.days

df['stars_per_day'] = df['stars'] / df['age_days']

df['issues_per_star'] = df['open_issues'] / df['stars']

df.to_csv('task1_metrics.csv', index=False)


# 1.3 Visualization 
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle('ML Frameworks Comparison: GitHub Metrics', fontsize=16)

sns.barplot(ax=axes[0], x='name', y='stars', data=df, palette='viridis')
axes[0].set_title('Total Stars')
axes[0].set_ylabel('Count')

sns.barplot(ax=axes[1], x='name', y='stars_per_day', data=df, palette='magma')
axes[1].set_title('Growth Velocity (Stars/Day)')
axes[1].set_ylabel('Stars per Day')

sns.barplot(ax=axes[2], x='name', y='issues_per_star', data=df, palette='rocket')
axes[2].set_title('Issues per Star Ratio')
axes[2].set_ylabel('Ratio')

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('task1_comparison.png')
plt.show()





#  Task 2 
# 2.1
import time
import pandas as pd
import requests
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_requests.log'),  # Saves to disk
        logging.StreamHandler(),  # Also print to console
    ],
)

logger = logging.getLogger(__name__)

def fetch_user_repos_paginated(username):
    """
    Fetch all repositories for a user with pagination.

    Args:
        username: GitHub username

    Returns:
        list: All repositories
    """
    all_repos = []
    page = 1
    per_page = 100  
    
    token = os.getenv('GITHUB_TOKEN')
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Library-Tutorial-App',
    }
    while True:
        try: 
            logger.info(f"Fetching page {page} for user {username}...")
            url = f'https://api.github.com/users/{username}/repos'
            params = {'page': page, 'per_page': per_page}
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 429: 
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            if not data: 
                logger.info(f"Reached the end, Total repos: {len(all_repos)}")
                break

            all_repos.extend(data)
            page  += 1
            time.sleep(1)
        except Exception as e: 
            logger.error(f"Error on page {page}: {e}")
            break 
    # Your implementation here
    # Remember to:
    # 1. Check for empty responses  --> means you've hit the last page
    # 2. Add delays                 --> time.sleep(1) to be polite
    # 3. Handle errors              --> try/except around requests
    # 4. Log progress               --> print or logger.info(f"Page {page}")

    return all_repos


# 2.2 Analyze the repositories
def analyze_and_report(repos, username):
    if not repos:
        logger.warning("No repos to analyze")
        return

    df = pd.DataFrame(repos)

    most_used_lang = df['language'].mode()[0] if not df['language'].dropna().empty else "N/A"

    avg_stars = df['stargazers_count'].mean()

    total_forks = df['forks_count'].sum()

    df['updated_at'] = pd.to_datetime(df['updated_at'])
    most_recent_repo = df.loc[df['updated_at'].idxmax(), 'name']

    df['created_at'] = pd.to_datetime(df['created_at'])
    oldest_repo = df.loc[df['created_at'].idxmin(), 'name']

    report = (
        f"GitHub Repository Analysis: {username}\n"
        f"{'='*40}\n"
        f"Most Used Language:      {most_used_lang}\n"
        f"Average Stars per Repo:  {avg_stars:.2f}\n"
        f"Total Forks:             {total_forks}\n"
        f"Most Recently Updated:   {most_recent_repo}\n"
        f"Oldest Repository:       {oldest_repo}\n"
        f"{'='*40}\n"
    )

    try:
        with open('task2_analysis.txt', 'w') as f:
            f.write(report)
        logger.info("Summary report saved as 'task2_analysis.txt'")
    except Exception as e:
        logger.error(f"Failed to save report: {e}")

if __name__ == "__main__":
    user = "pandas-dev" 
    all_data = fetch_user_repos_paginated(user)
    
    if all_data:
        df_raw = pd.DataFrame(all_data)
        df_raw.to_csv('task2_all_repos.csv', index=False)
        analyze_and_report(all_data, user)




# Task 3 
import os
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openpyxl.styles import Font, PatternFill, Alignment


class GitHubAnalyzer:
    """
    Complete GitHub API client with analysis capabilities.
    Build on top of the GitHubAPI class concepts from section 2.7.
    """

    def __init__(self, token=None):
        self.base_url = 'https://api.github.com'
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limiter = RateLimiter(max_requests=5000, time_window=3600)
        self.session = self._create_session()
        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})
        
        self.session.headers.update({
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHubAnalyzer/2.0',
        })

    def _create_session(self):
        """create session with retry logic for 429 and 5xx errors"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1, # Wait 1s, 2s, 4s...
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session
    
    def _get(self, endpoint, params=None):
        """Internal GET method with rate limiting and logging"""
        self.rate_limiter.wait_if_needed()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            self.logger.info(f"Successfully fetched {endpoint}")
            return response.json()
        except Exception as e:
            self.logger.error(f"Request failed for {endpoint}: {e}")
            raise

    def search_repos(self, query, language=None, min_stars=0):
        """
        Search repositories with filters.

        Returns:
            DataFrame with results
        """
        q_parts = [query]
        if language: 
            q_parts.append(f"language:{language}")
        if min_stars: 
            q_parts.append(f"stars:>={min_stars}")
        q = ' '.join(q_parts)
        data = self._get('/search/repositories', params={'q': q})
        return self._to_dataframe(data.get('items', []))
    def get_trending(self, language=None, since_days=30):
        """
        Get repositories created recently with high activity
        """
        date_cutoff = (datetime.now() - timedelta(days=since_days)).strftime('%Y-%m-%d')
        query = f"created:>{date_cutoff}"
        data = self._get('/search/repositories', params={'q': query, 'sort': 'stars', 'order': 'desc'})
        return self._to_dataframe(data.get('items', []))
    
    def compare_repos(self, repo_list):
        """
        Compare multiple repositories.

        Args:
            repo_list: List of "owner/repo" strings

        Returns:
            DataFrame with comparison
        """
        results = []
        for repo_path in repo_list:
            try:
                repo_data = self._get(f'/repos/{repo_path}')
                results.append(repo_data)
            except:
                continue # Skip failed fetches
        return self._to_dataframe(results)
    def _to_dataframe(self, items):
        """convert JSON list to structured DataFrame"""
        rows = []
        for item in items:
            rows.append({
                'name': item['name'],
                'full_name': item['full_name'],
                'stars': item['stargazers_count'],
                'forks': item['forks_count'],
                'open_issues': item['open_issues_count'],
                'language': item.get('language'),
                'created_at': item['created_at'],
                'updated_at': item['updated_at']
            })
        return pd.DataFrame(rows)
    
    def export_to_excel(self, df, filename):
        """
        Export DataFrame to Excel with formatting.
        - Bold headers
        - Auto-adjust column widths
        - Add creation timestamp
        """
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='GitHub_Analysis')
            
            workbook = writer.book
            worksheet = writer.sheets['GitHub_Analysis']
            
            header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
            header_font = Font(bold=True, color="FFFFFF")
            
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center')

            for col in worksheet.columns:
                max_length = 0
                column_letter = col[0].column_letter
                for cell in col:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                worksheet.column_dimensions[column_letter].width = max_length + 2

        self.logger.info(f"Exported data to {filename}")


# Test your class by:
# 1. Searching for "data science" repos in Python with >500 stars
# 2. Comparing 5 repos of your choice
# 3. Exporting results to task3_results.xlsx

if __name__ == "__main__":
    analyzer = GitHubAnalyzer(token=os.getenv('GITHUB_TOKEN'))

    search_df = analyzer.search_repos("data science", language="python", min_stars=500)

    repos_to_compare = [
        'tensorflow/tensorflow', 'pytorch/pytorch', 
        'scikit-learn/scikit-learn', 'numpy/numpy', 'pandas-dev/pandas'
    ]
    comparison_df = analyzer.compare_repos(repos_to_compare)

    analyzer.export_to_excel(comparison_df, 'task3_results.xlsx')
