import os
import csv
import glob
import sys

# Input path as a variable - set this to your base directory containing user folders
BASE_DIR = r"H:\Py\Copilot-Project\3. Data\Complete Data"

def extract_repo_info(base_dir):
    output_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "github_repos.csv")
    repos = []
    
    try:
        user_dirs = [d for d in glob.glob(os.path.join(base_dir, "*")) if os.path.isdir(d)]
        
        for user_dir in user_dirs:
            username = os.path.basename(user_dir)
            repo_data_dir = os.path.join(user_dir, "Repo Data")
            
            if not os.path.exists(repo_data_dir):
                continue
            
            json_files = glob.glob(os.path.join(repo_data_dir, "*.json"))
            
            for json_file in json_files:
                file_basename = os.path.basename(json_file)
                
                # Skip files with _forked.json suffix
                if "_forked.json" in file_basename:
                    continue
                
                if "_data.json" in file_basename:
                    repo_name = file_basename.replace("_data.json", "")
                else:
                    repo_name = os.path.splitext(file_basename)[0]
                    if repo_name.endswith("_data"):
                        repo_name = repo_name[:-5]
                
                # Construct GitHub URL
                github_url = f"https://github.com/{username}/{repo_name}"
                
                # Add to repos list
                repos.append({
                    'username': username,
                    'repo_name': repo_name,
                    'url': github_url
                })
        
        # Write to CSV file
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['username', 'repo_name', 'url']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for repo in repos:
                writer.writerow(repo)
        
        print(f"CSV file created: {output_csv}")
        return 0
    
    except Exception as e:
        import traceback
        print(f"Error: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(extract_repo_info(BASE_DIR))