<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesRentracks extends Command
{
    /** コマンド名 */
    protected $signature   = 'import:sales-rentracks';

    /** 説明 */
    protected $description = 'Rentracks 売上 CSV を csv_rentracks へ取込む';

    /* ---------- ヘルパ ---------- */
    /** 文字列日付 → Y-m-d H:i:s / null（曜日付き・区切り混在も許容） */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00' || $v === '-') return null;

        // ① 全角／半角スラッシュをハイフンへ統一
        $v = str_replace(['／', '/'], '-', $v);

        // ② “（木）” や “(Wed)” など括弧内の曜日・任意文字を除去
        $v = preg_replace('/[（(][^0-9]+?[）)]/u', '', $v);

        // ③ 余計な連続空白を 1 つに
        $v = preg_replace('/\s+/', ' ', $v);

        $ts = @strtotime($v);
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }


    /** 金額文字列 → 整数（円・税抜き） */
    private static function toIntExTax(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        $v = (float) str_replace(',', '', $v);
        return (int) floor($v / 1.1);   // 10% 消費税を除外
    }

    /** 金額文字列 → 整数（円） */
    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        return (int) floor((float) str_replace(',', '', $v));
    }

    /* ---------- 取込 ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Sales');          // 対象ディレクトリ
        $csvPattern = "$sourceDir/*rentrack*.csv";            // 対象ファイル
        $headers    = ['クリック日時'];                        // ヘッダー判定

        foreach (glob($csvPattern) as $csv) {
            /* SJIS → UTF-8 変換（BOM 保護） */
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers) {
                    $first = $row[0] ?? '';

                    // ①空行 ②ヘッダー行 は取り込まない
                    return (
                        (is_array($row) && count(array_filter($row, fn($v) => $v !== '')) === 0) ||
                        in_array($first, $headers, true)
                    );
                })
                ->each(function (array $row) {
                    DB::table('csv_rentracks')->insert([
                        'click_time'       => self::toDate($row[0]  ?? null),   // クリック日時
                        'occur_time'       => self::toDate($row[1]  ?? null),   // 売上日時
                        'order_id'         => $row[2]   ?? null,               // 売上番号
                        'ad_owner'         => $row[3]   ?? null,               // 広告主
                        'pg_name'          => $row[4]   ?? null,               // プロダクト
                        'sales'            => self::toInt($row[5]  ?? null),   // 売上額（税抜表記）
                        'reward'           => self::toIntExTax($row[6] ?? null), // 報酬額（税込→税抜）
                        'statement'        => $row[7]   ?? null,               // 状況
                        'approval_period'  => $row[8]   ?? null,               // 期限
                        'fix_time'         => self::toDate($row[9]  ?? null),   // 承認日
                        'note'             => $row[10]  ?? null,               // 備考
                        'site_id'          => $row[11]  ?? null,               // サイトID
                        'site_name'        => $row[12]  ?? null,               // サイト名
                        'device'           => $row[13]  ?? null,               // デバイス
                        'referer'          => $row[14]  ?? null,               // リファラー
                        'user_agent'       => $row[15]  ?? null,               // ユーザーエージェント
                        'reject_reason'    => $row[16]  ?? null,               // 拒否理由
                    ]);
                });

            unlink($tmp);
        }

        $this->info('Rentracks 売上データ取込完了');
        return Command::SUCCESS;
    }
}
