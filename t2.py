from camoufox.sync_api import Camoufox







with Camoufox(humanize=2.0,headless=False) as browser:
    #max tryies= 5
    tries = 0
    

    while True:
        browser.new_context()

        page = browser.new_page(java_script_enabled=True)
        page.goto("https://rubinot.com.br")
        browser.close()
        #get status code
        content=page.content()
        if "Just a moment..." not in content[:100]:
 
            print("Page loaded successfully")
            print(content)
            break
        else:
            print("Failed to load page")
            
            tries += 1
            if tries >= 5:
                print("Max retries reached. Exiting.")
                break
            continue
