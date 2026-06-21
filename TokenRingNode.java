import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;

public class TokenRingNode {
    // Variáveis de Configuração
    private static String meuApelido;
    private static int tempoDelaySeconds;
    private static int probabilidadeErro;
    private static double timeoutTokenSecs;
    private static double tempoMinimoTokensSecs;
    private static final int PORTA = 6000;
    private static String meuIp;
    // Topologia em Anel
    private static final TreeMap<String, InetAddress> rede = new TreeMap<>();
    private static String apelidoSucessor = null;
    private static InetAddress ipSucessor = null;
    
    // Estruturas de Dados
    private static final Queue<MensagemFila> filaMensagens = new LinkedList<>();
    private static DatagramSocket socket;
    
    // Controle de Token
    private static boolean possuiToken = false;
    private static boolean souControladorToken = false;
    private static long ultimoTempoRecebimentoToken = System.currentTimeMillis();
    private static boolean descartarProximoToken = false; // Para retirada manual do token
    
    public static void main(String[] args) {
        try {
            carregarConfiguracao(); ///adicionar o ip como parametro
            socket = new DatagramSocket(PORTA);
            socket.setBroadcast(true);

            // Inicia Thread para receber pacotes
            new Thread(TokenRingNode::escutarRede).start();

            // Adiciona a si mesmo na rede
            rede.put(meuApelido, InetAddress.getByName(meuIp));            
            System.out.println("[INICIO] Iniciando Nó " + meuApelido + " | Esperando formar anel...");

            // Envia DISCOVER em Broadcast
           // System.out.println("DISCOVER "+meuApelido+" " + meuApelido + " " + InetAddress.getLocalHost().getHostAddress()+ " ENVIADO");
            String discoverMsg = "10:" + meuApelido + ":" + meuIp;
            //enviarPacote("10:" + meuApelido + ":" + InetAddress.getLocalHost().getHostAddress(),InetAddress.getByName("255.255.255.255"));
            enviarPacote(discoverMsg, InetAddress.getByName("255.255.255.255"));

            // Inicia interface do usuário
            interfaceUsuario();

        } catch (Exception e) {
            System.err.println("Erro na inicialização: " + e.getMessage());
        }
    }

   private static void carregarConfiguracao() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("config.txt"));
        meuApelido = reader.readLine().trim();
        meuIp = reader.readLine().trim(); // Lê o IP da segunda linha
        tempoDelaySeconds = Integer.parseInt(reader.readLine().trim());
        probabilidadeErro = Integer.parseInt(reader.readLine().trim());
        
        timeoutTokenSecs = Double.parseDouble(reader.readLine().trim().replace(",", "."));
        tempoMinimoTokensSecs = Double.parseDouble(reader.readLine().trim().replace(",", "."));
        
        reader.close();
    }

    private static void atualizarSucessor() {
        if (rede.size() < 2) return;
        
        List<String> apelidos = new ArrayList<>(rede.keySet());
        int meuIndex = apelidos.indexOf(meuApelido);
        
        int nextIndex = (meuIndex + 1) % apelidos.size();
        apelidoSucessor = apelidos.get(nextIndex);
        ipSucessor = rede.get(apelidoSucessor);
        System.out.println("[SUCESSOR]: " + apelidoSucessor);
        

        // A primeira máquina gera o token e se torna a CONTROLADORA
        if (meuIndex == 0 && !possuiToken && rede.size() >= 2) {
            System.out.println("[SISTEMA] Sou a controladora. Gerando o Token inicial...");
            possuiToken = true;
            souControladorToken = true;
            ultimoTempoRecebimentoToken = System.currentTimeMillis();
            iniciarWatchdogToken(); // Inicia thread que monitora o timeout
            processarTurnoToken();
        }
    }

    private static void iniciarWatchdogToken() {
        new Thread(() -> {
            while (souControladorToken) {
                try {
                    Thread.sleep(1000);
                    long diff = System.currentTimeMillis() - ultimoTempoRecebimentoToken;
                    
                    // Verifica TIMEOUT (Token Perdido)
                    if (diff > (timeoutTokenSecs * 1000)) {
                        System.out.println("[CONTROLE-FALHA] Timeout do Token atingido (" + diff + "ms). O token foi perdido!");
                        System.out.println("[CONTROLE-FALHA] Gerando novo token...");
                        ultimoTempoRecebimentoToken = System.currentTimeMillis(); // reseta o tempo
                        processarTurnoToken();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();
    }

    private static void escutarRede() {
        try {
            byte[] buffer = new byte[2048];
            while (true) {
                DatagramPacket pct = new DatagramPacket(buffer, buffer.length);
                socket.receive(pct);
                
                String dadosRecebidos = new String(pct.getData(), 0, pct.getLength());
                String[] partes = dadosRecebidos.split(":");
                String codigo = partes[0];
                System.out.println("[RECEBIDO] " +dadosRecebidos);
                switch (codigo) {
                    case "10": // DISCOVER
                        String origemDesc = partes[1];
                        InetAddress ipDesc = InetAddress.getByName(partes[2]);
                        if (!origemDesc.equals(meuApelido)) {
                            rede.put(origemDesc, ipDesc);
                            atualizarSucessor();
                            System.out.println("[DISCOVER] "+meuApelido+" " + meuApelido + " " + meuIp + " ENVIADO");
                            enviarPacote("20:" + meuApelido + ":" + meuIp, ipDesc);
                        }
                        break;
                        
                    case "20": // HELLO
                        String origemHello = partes[1];
                        InetAddress ipHello = InetAddress.getByName(partes[2]);
                        if (!origemHello.equals(meuApelido)) {
                            rede.put(origemHello, ipHello);
                            atualizarSucessor();
                            System.out.println("[SUCESSOR pos update] "+ apelidoSucessor);
                        }
                        break;

                    case "1000": // TOKEN
                        if (descartarProximoToken) {
                            System.out.println("[MANUAL] Token recebido e removido da rede propositalmente.");
                            descartarProximoToken = false;
                            break;
                        }

                        if (souControladorToken) {
                            long agora = System.currentTimeMillis();
                            long tempoDesdeUltimo = agora - ultimoTempoRecebimentoToken;
                            
                            // Verifica MÚLTIPLOS TOKENS (Tempo Mínimo)
                            if (tempoDesdeUltimo < (tempoMinimoTokensSecs * 1000)) {
                                System.out.println("[CONTROLE-FALHA] Token chegou rápido demais (" + tempoDesdeUltimo + "ms). Há múltiplos tokens na rede!");
                                System.out.println("[CONTROLE-FALHA] Retirando este token excedente da rede.");
                                break; // Descarta este pacote e não repassa
                            }
                            // Atualiza o tempo com um token válido
                            ultimoTempoRecebimentoToken = agora;
                        }

                        if (apelidoSucessor != null) {
                            System.out.println("[TOKEN] Recebido. Analisando fila...");
                            processarTurnoToken();
                        }
                        break;

                    case "2000": // DADOS
                        processarPacoteDados(dadosRecebidos);
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processarTurnoToken() {
        try {
            // Se eu sou o controlador, reseto meu timer ao enviar algo pra frente pra evitar falsos timeouts imediatos
            if (souControladorToken) {
                 ultimoTempoRecebimentoToken = System.currentTimeMillis(); 
            }

            Thread.sleep(tempoDelaySeconds * 1000L); 

            if (filaMensagens.isEmpty()) {
                System.out.println("-> Repassando token para " + apelidoSucessor);
                enviarPacote("1000", ipSucessor);
            } else {
                MensagemFila msg = filaMensagens.peek();
                String pacote = gerarPacoteDados(msg);
                System.out.println("[ENVIO] Enviando dado para " + apelidoSucessor + " | Destino: " + msg.destino);
                enviarPacote(pacote, ipSucessor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processarPacoteDados(String pacoteCompleto) throws Exception {
        String[] p = pacoteCompleto.split(":", 6);
        String origem = p[1];
        String destino = p[2];
        String controle = p[3];
        long crcRecebido = Long.parseLong(p[4]);
        String mensagem = p[5];

        if (origem.equals(meuApelido)) {
            System.out.println("[RETORNO] Pacote retornou à origem. Status: " + controle);
            if (controle.equals("ACK") || controle.equals("maquinainexistente")) {
                if (controle.equals("maquinainexistente") && !destino.equals("BROADCAST")) {
                    System.out.println("[AVISO] Máquina " + destino + " inexistente/desligada.");
                }
                filaMensagens.poll(); 
            } else if (controle.equals("NAK")) {
                System.out.println("[RETRANSMISSÃO] Erro de CRC (NAK). Retransmitindo no próximo turno...");
            }
            enviarPacote("1000", ipSucessor); // Gira o token após tratar o pacote
            return;
        }

        if (destino.equals(meuApelido) || destino.equals("BROADCAST")) {
            System.out.println(">>> MENSAGEM de " + origem + ": " + mensagem);
            
            long crcCalculado = calcularCRC32(mensagem);
            if (destino.equals(meuApelido)) {
                if (crcCalculado == crcRecebido) {
                    controle = "ACK";
                } else {
                    controle = "NAK";
                    System.out.println("[ERRO] Falha no CRC identificada!");
                }
            }
        }

        String pacoteRepasse = "2000:" + origem + ":" + destino + ":" + controle + ":" + crcRecebido + ":" + mensagem;
        enviarPacote(pacoteRepasse, ipSucessor);
    }

    private static String gerarPacoteDados(MensagemFila msg) {
        String msgDados = msg.texto;
        
        if (new Random().nextInt(100) < probabilidadeErro) {
            msgDados += "_ERRO_SIMULADO";
        }
        
        long crc = calcularCRC32(msgDados);
        return "2000:" + meuApelido + ":" + msg.destino + ":maquinainexistente:" + crc + ":" + msgDados;
    }

    private static void interfaceUsuario() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("\nComandos disponíveis:");
        System.out.println("- Enviar Msg: <DESTINO> <MENSAGEM> (Ex: C Olá, mundo)");
        System.out.println("- Retirar token do anel: R (Remove ao receber)");
        System.out.println("- Injetar token forçado: G (Gera um token novo)");
        
        while (true) {
            String entrada = scanner.nextLine();
            
            if (entrada.equalsIgnoreCase("R")) {
                descartarProximoToken = true;
                System.out.println("[AÇÃO] O próximo token a chegar será removido do anel.");
            } else if (entrada.equalsIgnoreCase("G")) {
                if (apelidoSucessor == null) {
                    System.out.println("[ERRO] Você está sozinho na rede. Abra outras máquinas primeiro!");
                } else {
                    System.out.println("[AÇÃO] Gerando um novo token manualmente...");
                    try {
                        enviarPacote("1000", ipSucessor);
                    } catch (Exception e) {}
                }
            } else {
                String[] partes = entrada.split(" ", 2);
                if (partes.length == 2) {
                    String dest = partes[0].toUpperCase();
                    
                    // Trava 1: Verifica se o anel possui pelo menos mais uma máquina
                    if (apelidoSucessor == null) {
                        System.out.println("[AVISO] O anel ainda não foi formado. Inicie outras máquinas antes de enviar mensagens!");
                        continue;
                    }

                    // Trava 2: Aviso de máquina fora do radar local
                    if (!dest.equals("BROADCAST") && !rede.containsKey(dest)) {
                         System.out.println("[ALERTA LOCAL] A máquina '" + dest + "' não está mapeada no anel no momento.");
                         System.out.println("A mensagem será enviada mesmo assim para cumprir o protocolo de maquinainexistente.");
                    }

                    if (filaMensagens.size() < 10) {
                        filaMensagens.add(new MensagemFila(dest, partes[1]));
                        System.out.println("[FILA] (" + filaMensagens.size() + "/10) aguardando o token.");
                    } else {
                        System.out.println("[FILA] Erro: Fila cheia!");
                    }
                } else {
                     System.out.println("[ERRO] Formato inválido. Use: <DESTINO> <MENSAGEM>");
                }
            }
        }
    }

    private static void enviarPacote(String dados, InetAddress ip) throws Exception {
        System.out.println("[ENVIADO] "+ dados + " " + ip );
        if (ip != null) {
            byte[] buffer = dados.getBytes();
            DatagramPacket pct = new DatagramPacket(buffer, buffer.length, ip, PORTA);
            socket.send(pct);
        }
    }

    private static long calcularCRC32(String input) {
        CRC32 crc = new CRC32();
        crc.update(input.getBytes());
        return crc.getValue();
    }

    static class MensagemFila {
        String destino;
        String texto;
        MensagemFila(String destino, String texto) {
            this.destino = destino;
            this.texto = texto;
        }
    }
}