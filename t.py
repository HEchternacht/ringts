from rnet import Client, TlsOptions, TlsVersion, AlpnProtocol, ExtensionType
from rnet.emulation import Emulation
from rnet.emulation import EmulationOS,EmulationOption
import rnet
from rnet.http2 import Http2Options, PseudoId, PseudoOrder
from rnet.header import HeaderMap, OrigHeaderMap
import asyncio
from rnet import Client, Proxy  
from ipaddress import IPv4Address  
from rnet import Client  
from rnet.http2 import Http2Options, PseudoOrder, PseudoId, StreamDependency, StreamId  
  
http2_options = Http2Options(  
    initial_window_size=16777216,  
    initial_connection_window_size=16777216,  
    headers_pseudo_order=PseudoOrder(  
        PseudoId.METHOD,  
        PseudoId.PATH,   
        PseudoId.AUTHORITY,  
        PseudoId.SCHEME,  
    ),  
    headers_stream_dependency=StreamDependency(  
        StreamId.ZERO,  
        255,  
        False  
    ),  
)  
  
tls_options = TlsOptions(  
    grease_enabled=True,  # Enable GREASE to appear more browser-like  
    enable_ocsp_stapling=True,  
    curves_list=":".join(["X25519", "P-256", "P-384"]),  
    cipher_list=":".join([  
        "TLS_AES_128_GCM_SHA256",  
        "TLS_AES_256_GCM_SHA384",   
        "TLS_CHACHA20_POLY1305_SHA256",  
    ]),  
    alpn_protocols=[AlpnProtocol.HTTP2, AlpnProtocol.HTTP1],  
    min_tls_version=TlsVersion.TLS_1_2,  
    max_tls_version=TlsVersion.TLS_1_3,  
)  



tls_options = TlsOptions(  
    grease_enabled=True,  # Enable GREASE to appear more browser-like  
    enable_ocsp_stapling=True,  
    curves_list=":".join(["X25519", "P-256", "P-384"]),  
    cipher_list=":".join([  
        "TLS_AES_128_GCM_SHA256",  
        "TLS_AES_256_GCM_SHA384",   
        "TLS_CHACHA20_POLY1305_SHA256",  
    ]),  
    alpn_protocols=[AlpnProtocol.HTTP2, AlpnProtocol.HTTP1],  
    min_tls_version=TlsVersion.TLS_1_2,  
    max_tls_version=TlsVersion.TLS_1_3,  
)  
  






















async def a_request(url,proxy):
    print("creating client for proxy", proxy)
    try:
        client = Client(emulation=Emulation.Safari26,
                        proxies=[rnet.Proxy.all(proxy)]
    # Lower latency  
    )
        print("requesting via proxy", proxy)
        response = await client.get(url)

        print(response.status, "via proxy", proxy)
        return response
    except Exception as e:
        return f"error: {str(e)}"


def _request(url,proxy):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(a_request(url,proxy))
    loop.close()
    return result

from concurrent.futures import ThreadPoolExecutor, as_completed
def request(url,proxies):

    executor = ThreadPoolExecutor(max_workers=400)
    successful_responses = []
    try:
        future_to_proxy = {executor.submit(_request, url, proxy,): proxy for proxy in proxies}

        
        for future in as_completed(future_to_proxy):
            try:
                result = future.result()
                
                # Check if it's a successful response (not error string or stopped)
                if not isinstance(result, str) and result.status_code == 200:
                    print("successful response via proxy:", str(future_to_proxy[future]))
                    successful_responses.append((future_to_proxy[future], result))
                else:
                    print("unsuccessful response via proxy:", str(future_to_proxy[future]), "result:", result)
            except Exception as e:
                #log_console(f"Error with proxy {str(future_to_proxy[future])}: {str(e)}", "WARNING")
                continue
    
    except Exception as e:
        print("An error occurred:", e)
    finally:
        executor.shutdown(wait=True)
    
    return successful_responses

if __name__ == "__main__":


    import httpx

    API_KEY = "019bf3fc2b607ee086711c7bbe605634"  # Replace with your actual API key

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    url = "https://api.getfreeproxy.com/v1/proxies?protocol=http&anonymity=Transparent&limit=100" 

    response = httpx.get(url, headers=headers)
    pp=[p['proxyUrl'] for p in response.json()]
    pp

    with open("proxies2.txt", "w") as f:
        for proxy in pp:
            f.write(proxy + "\n")

    pp.append("http://ada.s.da.sd.a.sd.as")
    url="https://rubinothings.com.br/guild.php?guild=Ascended+Auroria&world=Auroria"
    url="https://rubinot.com.br"
    responses = request(url, pp)
    print(responses)
