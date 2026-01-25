from requests_html import HTMLSession,AsyncHTMLSession




async def main():
    asession = AsyncHTMLSession()
    targetUrl = "https://rubinothings.com.br/guild.php?guild=Ascended+Auroria&world=Auroria"



    r = await asession.get(targetUrl)
    await r.html.arender()
    print(r)
    print(r.text)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())